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
use service_engine::{BlobPolicy, BlobRef, OneShot, UploadUrl};
use sqlx::PgPool;
use uuid::Uuid;

const PAYLOAD: &[u8] = b"verified-bytes";
const SHA256_HEX: &str = "35b1135247e25b36525c4f5bb88038cf310f05ab54aa450e406e9ef5c5fde911";

async fn reference_of(pool: &PgPool, doc: Uuid) -> Uuid {
    sqlx::query_scalar("SELECT blob_ref FROM sample_doc WHERE id = $1")
        .bind(doc)
        .fetch_one(pool)
        .await
        .expect("read the blob reference")
}

#[tokio::test]
async fn s220_a_correct_checksum_of_a_different_length_is_refused_by_the_content_length_range() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s220-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let policy = BlobPolicy {
        max_bytes: 1 << 20,
        orphan_after: Duration::from_secs(3600),
    };
    let engine = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s220",
        "pod-s220",
        minio.config(&bucket),
        policy,
        Duration::from_secs(3600),
    )
    .await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let reader = engine.blob_reader();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    // The checksum is the real sha256 of PAYLOAD, but the declared size is larger:
    // the exact content-length-range must refuse the shorter upload.
    let doc = Uuid::now_v7();
    let upload: OneShot<UploadUrl> = executor
        .run::<AttachVerifiedDoc>(
            principal,
            AttachVerifiedDoc {
                id: doc,
                tenant,
                name: "short.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                size: (PAYLOAD.len() as u64) + 6,
                sha256_hex: SHA256_HEX.to_string(),
            },
        )
        .await
        .expect("the verified attach commits");
    let reference = reference_of(&pool, doc).await;

    let http = reqwest::Client::new();
    let status = post_upload(&http, upload.into_inner(), PAYLOAD.to_vec()).await;
    assert!(
        !status.is_success(),
        "the content-length-range refuses a body of the wrong length: {status}",
    );

    let head = reader
        .head(BlobRef(reference))
        .await
        .expect("head does not error");
    assert!(head.is_none(), "the rejected upload never landed");
    let state: String = sqlx::query_scalar("SELECT state FROM service_engine.blob WHERE id = $1")
        .bind(reference)
        .fetch_one(&pool)
        .await
        .expect("read state");
    assert_eq!(
        state, "pending",
        "the row stays pending, no object to promote"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
