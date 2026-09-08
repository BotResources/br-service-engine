use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::boot_blob_engine;
use service_engine::BlobPolicy;
use service_engine::error::EngineError;
use uuid::Uuid;

#[tokio::test]
async fn s45_a_registered_blob_kind_with_an_absent_bucket_fails_boot_loud() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s45-absent-{}", Uuid::now_v7().simple());

    let policy = BlobPolicy {
        max_bytes: 1 << 20,
        orphan_after: Duration::from_secs(3600),
    };
    let engine = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s45",
        "pod-s45",
        minio.config(&bucket),
        policy,
        Duration::from_secs(3600),
    )
    .await;

    let error = engine
        .run()
        .await
        .expect_err("run must fail loud when the object-storage bucket is absent");
    assert!(
        matches!(error, EngineError::BlobBucketAbsent { .. }),
        "the failure names the absent bucket rather than serving without storage: {error}",
    );

    drop(nats);
    drop(minio);
    db.cleanup().await;
}
