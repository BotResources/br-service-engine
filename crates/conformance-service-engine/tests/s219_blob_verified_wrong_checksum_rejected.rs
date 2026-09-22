#[allow(dead_code)]
mod blob_support;
#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use blob_support::post_upload;
use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::AttachVerifiedDoc;
use conformance_service_engine::sample::boot_blob_engine;
use conformance_service_engine::sample::render::member;
use engine_twin::await_ready;
use service_engine::blobs::Disposition;
use service_engine::{BlobPolicy, BlobRef, OneShot, UploadUrl};
use sqlx::PgPool;
use uuid::Uuid;

const PAYLOAD: &[u8] = b"verified-bytes";
const WRONG_SHA: &str = "0000000000000000000000000000000000000000000000000000000000000000";

async fn reference_of(pool: &PgPool, doc: Uuid) -> Uuid {
    sqlx::query_scalar("SELECT blob_ref FROM sample_doc WHERE id = $1")
        .bind(doc)
        .fetch_one(pool)
        .await
        .expect("read the blob reference")
}

async fn present(pool: &PgPool, id: Uuid) -> bool {
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM service_engine.blob WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("count");
    n > 0
}

async fn wait_gone(pool: &PgPool, id: Uuid) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while present(pool, id).await {
        assert!(tokio::time::Instant::now() < deadline, "not reaped in time");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn s219_bytes_whose_checksum_does_not_match_the_expectation_never_land_and_are_reaped() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s219-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let policy = BlobPolicy {
        max_bytes: 1 << 20,
        orphan_after: Duration::from_millis(400),
    };
    let engine = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s219",
        "pod-s219",
        minio
            .config(&bucket)
            .with_upload_ttl(Duration::from_millis(400)),
        policy,
        Duration::from_millis(150),
    )
    .await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let reader = engine.blob_reader();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let doc = Uuid::now_v7();
    let upload: OneShot<UploadUrl> = executor
        .run::<AttachVerifiedDoc>(
            principal,
            AttachVerifiedDoc {
                id: doc,
                tenant,
                name: "wrong.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                size: PAYLOAD.len() as u64,
                sha256_hex: WRONG_SHA.to_string(),
            },
        )
        .await
        .expect("the verified attach commits");
    let reference = reference_of(&pool, doc).await;

    let http = reqwest::Client::new();
    let status = post_upload(&http, upload.into_inner(), PAYLOAD.to_vec()).await;
    assert!(
        !status.is_success(),
        "MinIO refuses bytes whose checksum does not match the pinned expectation: {status}",
    );

    let head = reader
        .head(BlobRef(reference))
        .await
        .expect("head does not error");
    assert!(
        head.is_none(),
        "the object never landed, so head resolves to None"
    );
    let download = reader
        .download_url(BlobRef(reference), Disposition::Attachment)
        .await
        .expect("download does not error");
    assert!(
        download.is_none(),
        "a pending row with no object mints no download URL"
    );

    wait_gone(&pool, reference).await;

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
