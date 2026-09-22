#[allow(dead_code)]
mod blob_support;
#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use blob_support::post_upload;
use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::AttachVerifiedDoc;
use conformance_service_engine::sample::boot_blob_policy_engine;
use conformance_service_engine::sample::render::member;
use engine_twin::await_ready;
use service_engine::blobs::Disposition;
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
async fn s235_a_verified_pending_row_mints_no_download_until_it_is_promoted() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s235-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let policy = BlobPolicy {
        max_bytes: 1 << 20,
        orphan_after: Duration::from_secs(3600),
    };

    let quiet = boot_blob_policy_engine(
        &db,
        nats.nats().await,
        "se_s235",
        "pod-s235a",
        minio.config(&bucket),
        policy,
        Duration::from_secs(3600),
        false,
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
        .expect("the verified attach commits");
    let reference = reference_of(&pool, doc).await;

    let http = reqwest::Client::new();
    let status = post_upload(&http, upload.into_inner(), PAYLOAD.to_vec()).await;
    assert!(status.is_success(), "the exact bytes land: {status}");

    let pending = reader
        .download_url(BlobRef(reference), Disposition::Attachment)
        .await
        .expect("resolving a download URL does not error");
    assert!(
        pending.is_none(),
        "a verified pending row (an expectation and a post-upload policy) mints no download URL \
         before the reaper has judged it",
    );

    shutdown.notify_one();
    running.await.expect("join A").expect("A returns Ok");

    let engine = boot_blob_policy_engine(
        &db,
        nats.nats().await,
        "se_s235",
        "pod-s235b",
        minio.config(&bucket),
        policy,
        Duration::from_millis(150),
        false,
    )
    .await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let reader = engine.blob_reader();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let after = loop {
        let url = reader
            .download_url(BlobRef(reference), Disposition::Attachment)
            .await
            .expect("resolving a download URL does not error");
        if url.is_some() {
            break url;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the promoted verified row never became downloadable",
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        after.is_some(),
        "once promoted, the same reference resolves to a download URL",
    );

    shutdown.notify_one();
    running.await.expect("join B").expect("B returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
