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

#[tokio::test]
async fn s223_the_download_disposition_switches_between_inline_and_attachment() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s223-{}", Uuid::now_v7().simple());
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
        "se_s223",
        "pod-s223",
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

    let doc = Uuid::now_v7();
    let upload: OneShot<UploadUrl> = executor
        .run::<AttachDoc>(
            principal,
            AttachDoc {
                id: doc,
                tenant,
                name: "picture.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                fail: false,
            },
        )
        .await
        .expect("the attach commits");
    let reference = reference_of(&pool, doc).await;
    let http = reqwest::Client::new();
    let status = post_upload(&http, upload.into_inner(), b"bytes".to_vec()).await;
    assert!(status.is_success());

    let inline = reader
        .download_url(BlobRef(reference), Disposition::Inline)
        .await
        .expect("presign inline")
        .expect("the reference resolves")
        .into_string();
    let attachment = reader
        .download_url(BlobRef(reference), Disposition::Attachment)
        .await
        .expect("presign attachment")
        .expect("the reference resolves")
        .into_string();

    assert!(
        inline.contains("response-content-disposition=inline"),
        "the inline presign carries an inline content-disposition: {inline}",
    );
    assert!(
        attachment.contains("response-content-disposition=attachment"),
        "the attachment presign carries an attachment content-disposition: {attachment}",
    );
    assert_ne!(
        inline, attachment,
        "the two dispositions mint different response-content-disposition URLs",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
