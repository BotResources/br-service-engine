#[allow(dead_code)]
mod blob_support;
#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use blob_support::post_upload;
use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::blob::AttachDoc;
use conformance_service_engine::sample::boot_blob_engine;
use conformance_service_engine::sample::render::member;
use engine_twin::await_ready;
use service_engine::blobs::Disposition;
use service_engine::{BlobPolicy, BlobRef, OneShot, UploadUrl};
use sqlx::PgPool;
use uuid::Uuid;

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
async fn s224_upload_and_download_urls_carry_the_public_host_while_the_reaper_uses_the_internal_one()
 {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s224-{}", Uuid::now_v7().simple());
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
        "se_s224",
        "pod-s224",
        minio.config_with_public(&bucket),
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
        .run::<AttachDoc>(
            principal,
            AttachDoc {
                id: doc,
                tenant,
                name: "public.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                fail: false,
            },
        )
        .await
        .expect("the attach commits");
    let reference = reference_of(&pool, doc).await;
    let upload = upload.into_inner();
    assert!(
        upload.url().contains("localhost"),
        "the upload POST URL carries the public host: {}",
        upload.url(),
    );

    let http = reqwest::Client::new();
    let status = post_upload(&http, upload, b"public-bytes".to_vec()).await;
    assert!(
        status.is_success(),
        "the upload through the public host succeeds"
    );

    let download = reader
        .download_url(BlobRef(reference), Disposition::Attachment)
        .await
        .expect("presign download")
        .expect("the reference resolves")
        .into_string();
    assert!(
        download.contains("localhost"),
        "the download URL carries the public host: {download}",
    );

    // The reaper's HEAD runs against the internal host (127.0.0.1); a promotion proves it worked.
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

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
