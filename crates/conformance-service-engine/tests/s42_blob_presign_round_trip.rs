#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::blob::AttachDoc;
use conformance_service_engine::sample::boot_blob_engine;
use conformance_service_engine::sample::render::member;
use engine_twin::await_ready;
use service_engine::{BlobPolicy, BlobRef, OneShot};
use uuid::Uuid;

#[tokio::test]
async fn s42_a_presigned_upload_then_download_round_trips_the_bytes_through_real_minio() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s42-{}", Uuid::now_v7().simple());
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
        "se_s42",
        "pod-s42",
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
    let upload: OneShot<String> = executor
        .run::<AttachDoc>(
            principal,
            AttachDoc {
                id: doc,
                tenant,
                name: "photo.bin".to_string(),
                content_type: "application/octet-stream".to_string(),
                fail: false,
            },
        )
        .await
        .expect("the attach commits and returns the upload URL on the sync channel");

    let http = reqwest::Client::new();
    let payload = b"the-bytes-never-cross-the-pipeline".to_vec();
    let put = http
        .put(upload.into_inner())
        .body(payload.clone())
        .send()
        .await
        .expect("PUT to the presigned upload URL");
    assert!(put.status().is_success(), "MinIO accepted the upload");

    let reference: Uuid = sqlx::query_scalar("SELECT blob_ref FROM sample_doc WHERE id = $1")
        .bind(doc)
        .fetch_one(&pool)
        .await
        .expect("read the blob reference");
    let download = reader
        .download_url(BlobRef(reference))
        .await
        .expect("presign a download")
        .expect("the reference resolves to an object");
    let got = http
        .get(download.into_string())
        .send()
        .await
        .expect("GET the presigned download URL")
        .bytes()
        .await
        .expect("read the downloaded bytes");
    assert_eq!(got.as_ref(), payload.as_slice(), "the bytes round-trip");

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    drop(minio);
    db.cleanup().await;
}
