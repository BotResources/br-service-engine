use std::time::Duration;

use br_util_axum_readiness::{Readiness, ReadinessHandle};
use conformance_service_engine::infra::listener::engine_config;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::assignment::{Assignment, AssignmentProjector};
use conformance_service_engine::sample::principal::{SamplePrincipalResolver, SampleRls};
use service_engine::Engine;
use service_engine::housekeeping::ready::REASON_NATS_UNREACHABLE;

const READY_WITHIN: Duration = Duration::from_secs(25);
const DOWN_WITHIN: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(100);
const NATS_GRACE: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s29_a_nats_outage_past_its_grace_window_takes_the_pod_out_of_rotation_with_the_reason() {
    let db = TestDb::fresh().await;
    let mut nats = TestNats::spawn().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let config = engine_config("se_s29_nats_grace", "pod-nats-grace").with_nats_grace(NATS_GRACE);

    let readiness = ReadinessHandle::ready();
    let mut engine = Engine::boot(config, pool.clone(), fabric, readiness.clone())
        .await
        .expect("the engine boots while nats is reachable");
    engine.bind_noun::<Assignment>().expect("bind the noun");
    engine.register_rls(SampleRls).expect("register the rls");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the resolver");
    engine
        .register_projector(AssignmentProjector)
        .expect("register the projector");

    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    nats.stop();

    let reason = await_not_ready(&readiness).await;
    assert_eq!(
        reason, REASON_NATS_UNREACHABLE,
        "a nats outage past nats_grace must take the pod DOWN with the nats reason, not silently"
    );

    running.abort();
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
            "readiness never rose to UP while nats was reachable"
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
            "readiness stayed UP after nats died for longer than nats_grace plus detection"
        );
        tokio::time::sleep(POLL).await;
    }
}
