use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::boot_blob_engine;
use service_engine::BlobPolicy;
use service_engine::error::EngineError;
use uuid::Uuid;

#[tokio::test]
async fn s236_a_blob_kind_whose_orphan_window_is_shorter_than_the_upload_ttl_is_refused_at_bind() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let minio = TestMinio::spawn().await;
    let bucket = format!("se-s236-{}", Uuid::now_v7().simple());
    minio.create_bucket(&bucket).await;

    let policy = BlobPolicy {
        max_bytes: 1 << 20,
        orphan_after: Duration::from_secs(60),
    };
    let engine = boot_blob_engine(
        &db,
        nats.nats().await,
        "se_s236",
        "pod-s236",
        minio
            .config(&bucket)
            .with_upload_ttl(Duration::from_secs(900)),
        policy,
        Duration::from_secs(3600),
    )
    .await;

    let error = engine
        .run()
        .await
        .expect_err("an orphan window shorter than the upload TTL must fail boot loud");
    assert!(
        matches!(
            &error,
            EngineError::BlobOrphanWindowTooShort { kind, orphan_after, upload_ttl }
                if *kind == "attachment"
                    && *orphan_after == Duration::from_secs(60)
                    && *upload_ttl == Duration::from_secs(900)
        ),
        "the failure names the kind and both durations: {error:?}",
    );

    drop(nats);
    drop(minio);
    db.cleanup().await;
}
