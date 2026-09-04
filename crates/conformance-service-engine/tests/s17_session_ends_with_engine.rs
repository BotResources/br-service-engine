use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use br_util_axum_readiness::{Readiness, ReadinessHandle};
use conformance_service_engine::infra::listener::engine_config;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::assignment::{Assignment, AssignmentProjector};
use conformance_service_engine::sample::principal::{SamplePrincipalResolver, SampleRls};
use conformance_service_engine::sample::render::{
    assignment, attach_request, member, next_delta, window,
};
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use futures_util::StreamExt;
use service_engine::Engine;
use service_engine::error::EngineError;
use service_engine::impact::{Dims, Impact};
use service_engine::mirror::{MirrorHandle, MirrorRun};
use service_engine::name::MirrorName;
use service_engine::transport::{ImpactTransport, PgListenNotify};
use uuid::Uuid;

const READY_WITHIN: Duration = Duration::from_secs(25);
const STOPPED_WITHIN: Duration = Duration::from_secs(25);
const STREAM_END_WITHIN: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_millis(50);

fn a_mirror_whose_supervisor_dies_after_it_converges() -> MirrorHandle {
    let progress_reads = Arc::new(AtomicU64::new(0));
    MirrorHandle::new(
        MirrorName::from_static("directory"),
        || Box::pin(async { Ok(()) }) as MirrorRun,
        || {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(1)).await;
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
async fn s17_a_session_survives_no_longer_than_the_engine_when_a_worker_stops() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    assignment(&pool, home, "alpha").await;

    let readiness = ReadinessHandle::ready();
    let mut engine = Engine::boot(
        engine_config("se_s17_worker_stop", "pod-worker-stop"),
        pool.clone(),
        fabric,
        readiness.clone(),
    )
    .await
    .expect("the engine boots");
    engine.bind_noun::<Assignment>().expect("bind the noun");
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

    let render = engine.render();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let mut stream = render
        .attach(attach_request(
            &principal,
            vec![window(AssignmentProjector::NAME, false)],
        ))
        .await
        .expect("the session attaches while the engine is live");

    let outcome = tokio::time::timeout(STOPPED_WITHIN, running)
        .await
        .expect("run returns once its mirror supervisor dies")
        .expect("the engine task joins");
    assert!(
        matches!(
            outcome,
            Err(EngineError::WorkerStopped { worker: "mirror" })
        ),
        "a dead mirror supervisor stops the engine, got {outcome:?}"
    );

    loop {
        match tokio::time::timeout(STREAM_END_WITHIN, stream.next()).await {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(_) => panic!(
                "the engine stopped on a dead worker but a live session's stream never ended, so \
                 the client hangs on a pod that has left rotation"
            ),
        }
    }

    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s17_a_session_ends_when_the_render_worker_panics_the_one_path_a_sibling_await_cannot_close()
 {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let key = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let panic_switch = Arc::new(AtomicBool::new(false));
    let config = engine_config("se_s17_render_panic", "pod-render-panic");
    let readiness = ReadinessHandle::ready();
    let mut engine = Engine::boot(config.clone(), pool.clone(), fabric, readiness.clone())
        .await
        .expect("the engine boots");
    engine.bind_noun::<Assignment>().expect("bind the noun");
    engine.register_rls(SampleRls).expect("register the rls");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the resolver");
    engine
        .register_projector(SpyAssignments::new(spy).with_panic_switch(panic_switch.clone()))
        .expect("register the panicking projector");

    let render = engine.render();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let mut stream = render
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches while the engine is live");
    next_delta(&mut stream, STREAM_END_WITHIN)
        .await
        .expect("the opening Reset");

    panic_switch.store(true, Ordering::SeqCst);
    let stager = PgListenNotify::connect(pool.clone(), &config)
        .await
        .expect("a second connection stages an impact onto the engine's channel");
    let impact = Impact::resource::<Assignment>(&key, Dims::EMPTY).expect("a resource impact");
    let mut tx = pool.begin().await.expect("open a write transaction");
    stager
        .stage_in(&mut tx, std::slice::from_ref(&impact))
        .await
        .expect("stage the impact the render pass will project and panic on");
    tx.commit().await.expect("commit the staged impact");

    let outcome = tokio::time::timeout(STOPPED_WITHIN, running)
        .await
        .expect("run returns once its render worker panics mid-pass")
        .expect("the engine task joins");
    assert!(
        matches!(
            outcome,
            Err(EngineError::WorkerStopped { worker: "render" })
        ),
        "a panicking render pass stops the engine and names the render worker, got {outcome:?}"
    );

    loop {
        match tokio::time::timeout(STREAM_END_WITHIN, stream.next()).await {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(_) => panic!(
                "the render worker panicked, so run unwinds past render.run's own teardown and \
                 never re-awaits the dead task; unless run shuts the render runtime down on that \
                 path the attached session hangs forever on a departed pod"
            ),
        }
    }

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
            "readiness never rose to UP"
        );
        tokio::time::sleep(POLL).await;
    }
}
