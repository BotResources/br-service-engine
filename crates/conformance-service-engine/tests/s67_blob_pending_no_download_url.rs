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
use service_engine::{BlobPolicy, BlobRef, OneShot, UploadUrl};
use sqlx::PgPool;
use uuid::Uuid;

async fn reference_of(pool: &PgPool, doc: Uuid) -> Uuid {
    sqlx::query_scalar("SELECT blob_ref FROM sample_doc WHERE id = $1")
        .bind(doc)
        .fetch_one(pool)
        .await
        .expect("read the doc's blob reference")
}

#[tokio::test]
async fn s67_a_pending_blob_whose_object_never_landed_has_no_download_url() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s61-{}", Uuid::now_v7().simple());
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
        "se_s61",
        "pod-s61",
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
    let upload: OneShot<UploadUrl> = executor
        .run::<AttachDoc>(
            principal,
            AttachDoc {
                id: doc,
                tenant,
                name: "unposted.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                fail: false,
            },
        )
        .await
        .expect("attach commits the pending reference");
    let reference = BlobRef(reference_of(&pool, doc).await);

    let before = reader
        .download_url(reference)
        .await
        .expect("resolving a download URL does not error");
    assert!(
        before.is_none(),
        "a pending reference whose object never landed resolves to no download URL",
    );

    let status = post_upload(&http, upload.into_inner(), b"now-it-exists".to_vec()).await;
    assert!(status.is_success(), "the object lands");

    let after = reader
        .download_url(reference)
        .await
        .expect("resolving a download URL does not error");
    assert!(
        after.is_some(),
        "once the object has landed the same reference resolves to a download URL",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
