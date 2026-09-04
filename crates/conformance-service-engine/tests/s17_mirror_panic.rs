use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use br_util_axum_readiness::{Readiness, ReadinessHandle};
use conformance_service_engine::infra::listener::engine_config;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::assignment::{Assignment, AssignmentProjector};
use conformance_service_engine::sample::principal::{SamplePrincipalResolver, SampleRls};
use service_engine::Engine;
use service_engine::error::EngineError;
use service_engine::housekeeping::ready::REASON_WORKER_STOPPED;
use service_engine::mirror::{MirrorHandle, MirrorRun};
use service_engine::name::MirrorName;

const READY_WITHIN: Duration = Duration::from_secs(25);
const STOPPED_WITHIN: Duration = Duration::from_secs(25);
const POLL: Duration = Duration::from_millis(50);

fn a_mirror_whose_supervisor_dies_after_it_converges() -> MirrorHandle {
    let progress_reads = Arc::new(AtomicU64::new(0));
    MirrorHandle::new(
        MirrorName::from_static("directory"),
        || Box::pin(async { Ok(()) }) as MirrorRun,
        || {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                Err(EngineError::Service("the roster stream ended".into()))
            }) as MirrorRun
        },
    )
    .with_progress(move || {
        if progress_reads.fetch_add(1, Ordering::SeqCst) >= 1 {
            panic!("the mirror progress probe panicked outside the guarded steps");
        }
        0
    })
}

#[tokio::test]
async fn s17_a_supervisor_that_dies_after_converging_forces_readiness_down_and_run_returns_err() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    let fabric = nats.fabric().await;
    let readiness = ReadinessHandle::ready();

    let mut engine = Engine::boot(
        engine_config("se_s17_panic", "pod-mirror-panic"),
        db.app_pool().clone(),
        fabric,
        readiness.clone(),
    )
    .await
    .expect("the engine boots");
    engine
        .bind_noun::<Assignment>()
        .expect("bind the assignment noun");
    engine.register_rls(SampleRls).expect("register the rls");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the resolver");
    engine
        .register_projector(AssignmentProjector)
        .expect("register the projector");
    engine
        .register_mirror(a_mirror_whose_supervisor_dies_after_it_converges())
        .expect("register the mirror");

    let running = tokio::spawn(engine.run());

    await_state(
        &readiness,
        Readiness::Ready,
        READY_WITHIN,
        "readiness never rose to UP while the mirror was converged",
    )
    .await;

    let outcome = tokio::time::timeout(STOPPED_WITHIN, running)
        .await
        .expect("run returned once its mirror supervisor died")
        .expect("the engine task joined");
    match outcome {
        Err(EngineError::WorkerStopped { worker }) => assert_eq!(
            worker, "mirror",
            "a dead mirror supervisor is reported as a stopped worker, never left serving over a \
             board frozen at Converged"
        ),
        other => panic!("expected WorkerStopped naming the mirror, got {other:?}"),
    }
    assert_eq!(
        readiness.snapshot(),
        Readiness::NotReady {
            reason: REASON_WORKER_STOPPED.to_string()
        },
        "a mirror supervisor that dies must flip readiness DOWN so the pod leaves rotation"
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
