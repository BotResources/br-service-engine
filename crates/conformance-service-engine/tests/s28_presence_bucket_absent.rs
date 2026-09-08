use br_util_axum_readiness::Readiness;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_presence_engine;
use service_engine::error::EngineError;

const SERVICE: &str = "s28absent";
const CHANNEL: &str = "se_s28_absent";

#[tokio::test]
async fn s28_a_registered_presence_lane_with_no_bucket_fails_boot_loudly() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let engine = boot_presence_engine(&db, nats.nats().await, CHANNEL, "pod-absent", SERVICE).await;
    let readiness = engine.readiness();

    let outcome = engine.run().await;

    let error = outcome.expect_err("run must fail loud when the EPHEMERAL bucket is absent");
    assert!(
        matches!(error, EngineError::Posture(ref reason) if reason.contains("EPHEMERAL_s28absent")),
        "the failure must name the missing bucket, got {error:?}"
    );
    assert_ne!(
        readiness.snapshot(),
        Readiness::Ready,
        "a pod that could not bind its presence bucket never becomes ready"
    );

    drop(nats);
    db.cleanup().await;
}
