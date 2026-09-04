use std::time::Duration;

use br_util_axum_readiness::{Readiness, ReadinessHandle};
use conformance_service_engine::infra::listener::engine_config;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::assignment::{Assignment, AssignmentProjector};
use conformance_service_engine::sample::principal::{SamplePrincipalResolver, SampleRls};
use service_engine::Engine;
use service_engine::error::EngineError;
use service_engine::housekeeping::ready::REASON_WORKER_STOPPED;

const READY_WITHIN: Duration = Duration::from_secs(25);
const STOPPED_WITHIN: Duration = Duration::from_secs(25);
const POLL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn s27_a_worker_that_stops_before_shutdown_takes_the_pod_down_and_run_returns_err() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    let fabric = nats.fabric().await;
    let victim = db
        .pool_as(db.app_role())
        .await
        .expect("a pool the engine owns and the test can close underneath it");
    let readiness = ReadinessHandle::ready();

    let mut engine = Engine::boot(
        engine_config("se_s27_impact", "pod-supervised"),
        victim.clone(),
        fabric,
        readiness.clone(),
    )
    .await
    .expect("the engine boots under the low-privilege app role");
    engine
        .bind_noun::<Assignment>()
        .expect("bind the assignment noun");
    engine
        .register_rls(SampleRls)
        .expect("register the RLS applier");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(AssignmentProjector)
        .expect("register the assignment projector");

    let running = tokio::spawn(engine.run());

    await_state(
        &readiness,
        Readiness::Ready,
        READY_WITHIN,
        "readiness never rose to UP",
    )
    .await;

    victim.close().await;

    let outcome = tokio::time::timeout(STOPPED_WITHIN, running)
        .await
        .expect("run returned once its render worker lost the closed pool")
        .expect("the engine task joined");
    match outcome {
        Err(EngineError::WorkerStopped { worker }) => assert_eq!(
            worker, "render",
            "the render worker is the one whose stream ended on the closed pool"
        ),
        other => panic!("expected a WorkerStopped error naming the dead worker, got {other:?}"),
    }
    assert_eq!(
        readiness.snapshot(),
        Readiness::NotReady {
            reason: REASON_WORKER_STOPPED.to_string()
        },
        "a dead worker must flip readiness DOWN rather than leave the pod serving over a stopped \
         loop; the operator reason is fixed and the dead worker travels in the typed error"
    );

    drop(nats);
    db.cleanup().await;
}

async fn await_state(readiness: &ReadinessHandle, want: Readiness, within: Duration, whine: &str) {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        if readiness.snapshot() == want {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "{whine}");
        tokio::time::sleep(POLL).await;
    }
}
