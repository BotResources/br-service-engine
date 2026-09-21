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
use service_engine::blobs::BlobState;
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

async fn wait_until<F>(mut check: F)
where
    F: AsyncFnMut() -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if check().await {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "did not converge");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn s218_a_verified_upload_lands_and_head_reads_the_storage_checksum_in_the_pending_window() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s218-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let policy = BlobPolicy {
        max_bytes: 1 << 20,
        orphan_after: Duration::from_secs(3600),
    };

    let quiet = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s218",
        "pod-s218a",
        minio.config(&bucket),
        policy,
        Duration::from_secs(3600),
    )
    .await;
    let readiness = quiet.readiness();
    let shutdown = quiet.shutdown_handle();
    let executor = quiet.mutation_executor();
    let reader = quiet.blob_reader();
    let running = tokio::spawn(quiet.run());
    await_ready(&readiness).await;

    let doc = Uuid::now_v7();
    let upload: OneShot<UploadUrl> = executor
        .run::<AttachVerifiedDoc>(
            principal,
            AttachVerifiedDoc {
                id: doc,
                tenant,
                name: "verified.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                size: PAYLOAD.len() as u64,
                sha256_hex: SHA256_HEX.to_string(),
            },
        )
        .await
        .expect("the verified attach commits and returns the upload URL");
    let reference = reference_of(&pool, doc).await;

    let http = reqwest::Client::new();
    let status = post_upload(&http, upload.into_inner(), PAYLOAD.to_vec()).await;
    assert!(
        status.is_success(),
        "MinIO accepts the exact bytes: {status}"
    );

    let head = reader
        .head(BlobRef(reference))
        .await
        .expect("head does not error")
        .expect("the object has landed, so head resolves in the pending window");
    assert_eq!(
        head.state,
        BlobState::Pending,
        "the reaper has not swept yet"
    );
    assert_eq!(
        head.size,
        PAYLOAD.len() as u64,
        "size comes from live storage"
    );
    assert!(!head.etag.is_empty(), "etag comes from live storage");
    assert_eq!(
        head.sha256.map(|d| d.to_hex()),
        Some(SHA256_HEX.to_string()),
        "the checksum comes from live storage, not the still-null row column",
    );
    assert_eq!(
        head.verified(),
        Some(true),
        "size and checksum match the expectation"
    );

    let (row_size, row_sha): (Option<i64>, Option<Vec<u8>>) =
        sqlx::query_as("SELECT size, sha256 FROM service_engine.blob WHERE id = $1")
            .bind(reference)
            .fetch_one(&pool)
            .await
            .expect("read the row columns");
    assert!(
        row_size.is_none() && row_sha.is_none(),
        "the row's own columns stay NULL until the reaper promotes it",
    );

    shutdown.notify_one();
    running.await.expect("join A").expect("A returns Ok");

    let engine = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s218",
        "pod-s218b",
        minio.config(&bucket),
        policy,
        Duration::from_millis(150),
    )
    .await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let reader = engine.blob_reader();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    wait_until(async || {
        let state: Option<String> =
            sqlx::query_scalar("SELECT state FROM service_engine.blob WHERE id = $1")
                .bind(reference)
                .fetch_one(&pool)
                .await
                .expect("read state");
        state.as_deref() == Some("uploaded")
    })
    .await;

    let (row_size, row_etag, row_sha): (Option<i64>, Option<String>, Option<Vec<u8>>) =
        sqlx::query_as("SELECT size, etag, sha256 FROM service_engine.blob WHERE id = $1")
            .bind(reference)
            .fetch_one(&pool)
            .await
            .expect("read the promoted row");
    assert_eq!(
        row_size,
        Some(PAYLOAD.len() as i64),
        "size recorded on promotion"
    );
    assert!(
        row_etag.is_some_and(|e| !e.is_empty()),
        "etag recorded on promotion"
    );
    assert!(row_sha.is_some(), "sha256 recorded on promotion");

    let head = reader
        .head(BlobRef(reference))
        .await
        .expect("head does not error")
        .expect("head resolves after promotion");
    assert_eq!(head.state, BlobState::Uploaded, "the row is now uploaded");

    shutdown.notify_one();
    running.await.expect("join B").expect("B returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
