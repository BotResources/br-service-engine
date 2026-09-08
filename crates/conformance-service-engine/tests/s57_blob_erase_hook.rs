#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::blob::AttachOwnedDoc;
use conformance_service_engine::sample::boot_blob_engine;
use conformance_service_engine::sample::render::member;
use engine_twin::await_ready;
use service_engine::{BlobPolicy, OneShot, PersonId};
use sqlx::PgPool;
use uuid::Uuid;

async fn blob_present(pool: &PgPool, id: Uuid) -> bool {
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM service_engine.blob WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("count blob references");
    n > 0
}

#[tokio::test]
async fn s57_the_erase_hook_purges_a_persons_blobs_from_storage_and_the_reference_table() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s57-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let person = Uuid::now_v7();
    let principal = member(&pool, person, tenant).await;
    let policy = BlobPolicy {
        max_bytes: 1 << 20,
        orphan_after: Duration::from_secs(3600),
    };
    let engine = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s57",
        "pod-s57",
        minio.config(&bucket),
        policy,
        Duration::from_secs(3600),
    )
    .await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let reader = engine.blob_reader();
    let http = reqwest::Client::new();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let doc = Uuid::now_v7();
    let upload: OneShot<String> = executor
        .run::<AttachOwnedDoc>(
            principal,
            AttachOwnedDoc {
                id: doc,
                tenant,
                name: "mine.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                owner: person,
            },
        )
        .await
        .expect("attach an owned blob");
    http.put(upload.into_inner())
        .body(b"personal-bytes".to_vec())
        .send()
        .await
        .expect("upload the owned blob's bytes");

    let reference: Uuid = sqlx::query_scalar("SELECT blob_ref FROM sample_doc WHERE id = $1")
        .bind(doc)
        .fetch_one(&pool)
        .await
        .expect("read the blob reference");
    let object_key = format!("blob/attachment/{reference}");
    assert!(minio.object_exists(&bucket, &object_key).await);

    let purged = reader
        .purge_person(PersonId(person))
        .await
        .expect("the erase hook purges the person's blobs");
    assert_eq!(purged, 1, "one owned blob was purged");
    assert!(
        !blob_present(&pool, reference).await,
        "the reference row is gone"
    );
    assert!(
        !minio.object_exists(&bucket, &object_key).await,
        "the person's object is gone from storage",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
