use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::SamplePrincipal;
use conformance_service_engine::sample::engine_config;
use conformance_service_engine::sample::principal::SamplePrincipalResolver;
use service_engine::housekeeping::ready::REASON_SCHEMA_VERSION_DISPLACED;
use service_engine::{Engine, Readiness, ReadinessHandle};

const BEAT: Duration = Duration::from_millis(100);
const READY_WITHIN: Duration = Duration::from_secs(20);
const DOWN_WITHIN: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn s187_a_displaced_pod_goes_down_when_its_heartbeat_updates_no_row() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let readiness = ReadinessHandle::not_ready("boot");
    let mut engine = Engine::<SamplePrincipal>::boot(
        engine_config("se_s187", "pod-s187").with_beat(BEAT),
        pool.clone(),
        nats.nats().await,
        readiness.clone(),
    )
    .await
    .expect("the engine boots and claims the schema-version singleton");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    sqlx::query(
        "UPDATE service_engine.schema_version \
           SET engine_version = 'displaced-engine', service_version = 'displaced-service', \
               heartbeat = now() \
         WHERE singleton",
    )
    .execute(&pool)
    .await
    .expect("simulate another version claiming the singleton while this pod runs");

    let reason = await_not_ready(&readiness).await;
    assert_eq!(
        reason, REASON_SCHEMA_VERSION_DISPLACED,
        "the beat's heartbeat now updates no row, so the pod recognises it was displaced and goes \
         DOWN rather than keep serving over a store another version owns",
    );

    shutdown.notify_one();
    running
        .await
        .expect("the pod task joins")
        .expect("the pod run returns Ok");
    drop(nats);
    db.cleanup().await;
}

async fn await_ready(readiness: &ReadinessHandle) {
    let deadline = tokio::time::Instant::now() + READY_WITHIN;
    loop {
        if readiness.snapshot() == Readiness::Ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "readiness never rose to UP after a clean boot"
        );
        tokio::time::sleep(POLL).await;
    }
}

async fn await_not_ready(readiness: &ReadinessHandle) -> String {
    let deadline = tokio::time::Instant::now() + DOWN_WITHIN;
    loop {
        if let Readiness::NotReady { reason } = readiness.snapshot() {
            return reason;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "readiness stayed UP after the pod was displaced from the schema-version singleton"
        );
        tokio::time::sleep(POLL).await;
    }
}
