#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::erase::{AttachNoteBlob, boot_erase_blob_engine};
use conformance_service_engine::sample::render::member;
use engine_twin::await_ready;
use service_engine::{BlobPolicy, OneShot, PersonId};
use sqlx::PgPool;
use uuid::Uuid;

const SERVICE: &str = "s68blob";

async fn note_blob_ref(pool: &PgPool, note: Uuid) -> Option<Uuid> {
    sqlx::query_scalar::<_, Option<Uuid>>("SELECT blob_ref FROM sample_erase_note WHERE id = $1")
        .bind(note)
        .fetch_optional(pool)
        .await
        .expect("read the note's blob reference")
        .flatten()
}

async fn blob_present(pool: &PgPool, id: Uuid) -> bool {
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM service_engine.blob WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("count blob references");
    n > 0
}

#[tokio::test]
async fn s68_erase_purges_the_persons_blobs_from_storage_and_the_reference_table() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s68-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let person = Uuid::now_v7();
    let principal = member(&pool, person, tenant).await;
    let policy = BlobPolicy {
        max_bytes: 1 << 20,
        orphan_after: Duration::from_secs(3600),
    };
    let engine = boot_erase_blob_engine(
        &db,
        nats.nats().await,
        "se_s68",
        "pod-s68",
        SERVICE,
        minio.config(&bucket),
        policy,
    )
    .await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let eraser = engine.eraser();
    let http = reqwest::Client::new();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let note = Uuid::now_v7();
    let upload: OneShot<String> = executor
        .run::<AttachNoteBlob>(
            principal,
            AttachNoteBlob {
                note,
                owner: person,
                tenant,
                body: "mine.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
            },
        )
        .await
        .expect("attach a blob to the person's note");
    http.put(upload.into_inner())
        .body(b"personal-bytes".to_vec())
        .send()
        .await
        .expect("upload the blob's bytes");

    let reference = note_blob_ref(&pool, note)
        .await
        .expect("the note references a blob");
    let object_key = format!("{SERVICE}/attachment/{reference}");
    assert!(minio.object_exists(&bucket, &object_key).await);

    let outcome = eraser
        .erase(PersonId(person))
        .await
        .expect("the erase gesture purges the person's blobs");
    assert!(
        outcome.blobs_purged >= 1,
        "the released blob of the erased note is purged"
    );
    assert!(
        !blob_present(&pool, reference).await,
        "the reference row is gone after commit"
    );
    assert!(
        !minio.object_exists(&bucket, &object_key).await,
        "the person's object is gone from storage after commit",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
