#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::assignment::Assignment;
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::note::Note;
use conformance_service_engine::sample::principal::{
    FailingPrincipalResolver, SamplePrincipal, SampleRls,
};
use conformance_service_engine::sample::render::{
    assignment, attach_request, member, next_delta, window,
};
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use engine_twin::{SOON, await_ready, stage};
use futures_util::StreamExt;
use service_engine::Engine;
use service_engine::ReadinessHandle;
use service_engine::impact::{Deps, Impact};
use service_engine::principal::Principal;
use uuid::Uuid;

#[tokio::test]
async fn s131_a_principal_facts_change_whose_refresh_errors_ends_the_session_through_the_running_engine()
 {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    assignment(&pool, home, "alpha").await;

    let mut engine = Engine::<SamplePrincipal>::boot(
        engine_config("se_s131", "pod-s131"),
        pool.clone(),
        fabric,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the engine boots under the app role");
    engine.bind_noun::<Assignment>().expect("bind assignment");
    engine.bind_noun::<Note>().expect("bind note");
    engine
        .register_rls(SampleRls)
        .expect("register the RLS applier");
    engine
        .register_principal_resolver(FailingPrincipalResolver)
        .expect("register the refresh resolver that always errors");
    engine
        .register_projector(SpyAssignments::new(Spy::new()))
        .expect("register the sample projector");

    let readiness = engine.readiness();
    let transport = engine.transport_arc();
    let shutdown = engine.shutdown_handle();
    let render = engine.render();

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches with the passport-provided principal");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;
    next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");
    assert_eq!(render.live_sessions().await, 1);

    stage(
        &pool,
        transport.as_ref(),
        &[Impact::principal_facts(
            principal.id(),
            Deps::bit(0).unwrap(),
        )],
    )
    .await;

    assert!(
        tokio::time::timeout(SOON, stream.next())
            .await
            .expect("the stream ends rather than hanging")
            .is_none(),
        "the running engine re-resolves the principal on the staged PrincipalFactsChanged, the \
         resolver errors, and the session is ended fail-closed rather than served under stale facts",
    );
    assert_eq!(
        render.live_sessions().await,
        0,
        "no session of the principal survives a refresh whose resolver errored",
    );

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
