use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use br_util_axum_readiness::{Readiness, ReadinessHandle};
use conformance_service_engine::infra::listener::{engine_config, pool_named};
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::assignment::Assignment;
use conformance_service_engine::sample::principal::{SamplePrincipalResolver, SampleRls};
use conformance_service_engine::sample::render::{
    assignment, attach_request, member, next_delta, window,
};
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::Engine;
use service_engine::delta::Delta;
use uuid::Uuid;

const READY_WITHIN: Duration = Duration::from_secs(25);
const SOON: Duration = Duration::from_secs(3);
const QUIET: Duration = Duration::from_millis(300);
const OUTAGE_SETTLES: Duration = Duration::from_secs(3);
const POLL: Duration = Duration::from_millis(25);
const REPAIR_LISTENER: &str = "se_s13_repair_listener";

#[tokio::test]
async fn s13_the_beat_alone_repairs_a_pending_session_with_no_impact_after_the_outage() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let switch = Arc::new(AtomicBool::new(false));
    let config = engine_config("se_s13_repair", "pod-repair")
        .with_beat(Duration::from_millis(500))
        .with_lease(Duration::from_secs(30))
        .with_repair_attempts(50);
    let listen_pool = pool_named(&db, db.app_role(), REPAIR_LISTENER).await;
    let mut engine = Engine::boot(
        config,
        listen_pool.clone(),
        fabric,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the engine boots under the app role");
    engine.bind_noun::<Assignment>().expect("bind the noun");
    engine.register_rls(SampleRls).expect("register RLS");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the resolver");
    engine
        .register_projector(SpyAssignments::new(spy.clone()).with_fail_switch(switch.clone()))
        .expect("register the projector");

    let render = engine.render();
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let mut stream = render
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let opening = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert_eq!(opening.revision().get(), 1);

    switch.store(true, Ordering::Relaxed);
    db.terminate_backends(REPAIR_LISTENER).await;
    let deadline = tokio::time::Instant::now() + OUTAGE_SETTLES;
    while tokio::time::Instant::now() < deadline {
        assert!(
            next_delta(&mut stream, QUIET).await.is_none(),
            "a reconnect whose gated load failed is never handed a Reset built from the pre-gap \
             view, no matter how the listener connection dropped"
        );
    }

    switch.store(false, Ordering::Relaxed);
    let repaired = next_delta(&mut stream, SOON).await.expect(
        "with no impact after the outage, the housekeeping beat alone retries the pending repair",
    );
    assert!(
        matches!(repaired, Delta::Reset { .. }),
        "the beat-driven repair delivers the fresh Reset, not the stale pre-gap state: {repaired:?}"
    );
    assert!(
        repaired.revision().get() > 1,
        "the fresh Reset advances the session revision"
    );

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok on a clean shutdown");
    listen_pool.close().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s13_the_beat_ends_a_session_whose_repair_never_succeeds() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let switch = Arc::new(AtomicBool::new(false));
    let config = engine_config("se_s13_doomed", "pod-doomed")
        .with_beat(Duration::from_millis(200))
        .with_lease(Duration::from_secs(30))
        .with_repair_attempts(3);
    let mut engine = Engine::boot(config, pool.clone(), fabric, ReadinessHandle::ready())
        .await
        .expect("the engine boots under the app role");
    engine.bind_noun::<Assignment>().expect("bind the noun");
    engine.register_rls(SampleRls).expect("register RLS");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the resolver");
    engine
        .register_projector(SpyAssignments::new(spy.clone()).with_fail_switch(switch.clone()))
        .expect("register the projector");

    let render = engine.render();
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let mut stream = render
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    next_delta(&mut stream, SOON).await.expect("a Reset");

    switch.store(true, Ordering::Relaxed);
    render
        .resnapshot_all()
        .await
        .expect("resnapshot_all returns Ok while a session cannot be re-snapshotted");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if render.live_sessions().await == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "a session whose repair never succeeds must be ended by the beat after the bounded \
             attempts, not left live on its stale pre-gap view"
        );
        tokio::time::sleep(POLL).await;
    }
    assert!(
        next_delta(&mut stream, SOON).await.is_none(),
        "the ended session's stream ends explicitly rather than going silent"
    );

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok on a clean shutdown");
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
