use std::time::Duration;

use br_util_axum_readiness::{Readiness, ReadinessHandle};
use conformance_service_engine::infra::listener::engine_config;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::assignment::Assignment;
use conformance_service_engine::sample::gate::Gate;
use conformance_service_engine::sample::principal::{SamplePrincipalResolver, SampleRls};
use conformance_service_engine::sample::render::{assignment, attach_request, member, window};
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::Engine;
use service_engine::error::AttachError;
use uuid::Uuid;

const READY_WITHIN: Duration = Duration::from_secs(25);
const STOPPED_WITHIN: Duration = Duration::from_secs(25);
const POLL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn s02_an_attach_that_finalizes_after_engine_shutdown_is_refused_never_left_live() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let gate = Gate::new();
    let readiness = ReadinessHandle::ready();
    let mut engine = Engine::boot(
        engine_config("se_s02_attach_shutdown", "pod-attach-shutdown"),
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
        .register_projector(SpyAssignments::new(spy).gated(gate.clone()))
        .expect("register the gated projector");

    let render = engine.render();
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let attaching = {
        let render = render.clone();
        let request = attach_request(&principal, vec![window(SpyAssignments::NAME, false)]);
        tokio::spawn(async move { render.attach(request).await })
    };

    gate.wait_until_inside().await;
    shutdown.notify_waiters();

    let outcome = tokio::time::timeout(STOPPED_WITHIN, running)
        .await
        .expect("run returns once shutdown is signalled")
        .expect("the engine task joins");
    assert!(
        outcome.is_ok(),
        "a signalled shutdown returns run cleanly, got {outcome:?}"
    );

    gate.release();

    let attached = attaching.await.expect("the attach task joins");
    assert!(
        matches!(attached, Err(AttachError::ShuttingDown)),
        "an attach whose snapshot finalized after the engine shut down beneath it must be \
         refused, never taken live on a pod that has left rotation, got {attached:?}"
    );
    assert_eq!(
        render.live_sessions().await,
        0,
        "the refused attach leaves no session live behind an engine that has stopped"
    );

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
