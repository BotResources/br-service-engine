#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use engine_twin::{SOON, await_ready, spy_engine, stage};
use futures_util::StreamExt;
use service_engine::delta::Delta;
use service_engine::impact::{Deps, Impact};
use service_engine::principal::Principal;
use uuid::Uuid;

#[tokio::test]
async fn s22_engine_revoking_a_local_fact_drops_the_row_on_the_same_pass() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let exile = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let engine = spy_engine(
        &db,
        fabric,
        engine_config("se_s22_revoke", "pod-s22a"),
        SpyAssignments::new(Spy::new()),
    )
    .await;
    let readiness = engine.readiness();
    let transport = engine.transport_arc();
    let shutdown = engine.shutdown_handle();

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, true)],
        ))
        .await
        .expect("the RLS session attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;
    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");
    assert_eq!(assignment_ids(reset_views(&reset)), vec![subject]);

    move_member(&pool, principal.id().as_uuid(), exile).await;
    stage(
        &pool,
        transport.as_ref(),
        &[Impact::principal_facts(
            principal.id(),
            Deps::bit(0).unwrap(),
        )],
    )
    .await;
    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("revoking a local fact drops the row with no resource mutation");
    assert!(matches!(delta, Delta::Remove { .. }));
    assert_eq!(removed_key(&delta), subject);
    assert_eq!(delta.revision().get(), 2);

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s22_engine_a_resolver_returning_none_ends_every_session_of_that_principal() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    assignment(&pool, home, "alpha").await;

    let engine = spy_engine(
        &db,
        fabric,
        engine_config("se_s22_revoked", "pod-s22b"),
        SpyAssignments::new(Spy::new()),
    )
    .await;
    let readiness = engine.readiness();
    let transport = engine.transport_arc();
    let shutdown = engine.shutdown_handle();
    let render = engine.render();

    let mut first = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the first session attaches");
    let mut second = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the second session of the same principal attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;
    next_delta(&mut first, SOON).await.expect("first Reset");
    next_delta(&mut second, SOON).await.expect("second Reset");
    assert_eq!(render.live_sessions().await, 2);

    forget_member(&pool, principal.id().as_uuid()).await;
    stage(
        &pool,
        transport.as_ref(),
        &[Impact::principal_facts(principal.id(), Deps::EMPTY)],
    )
    .await;
    assert!(
        tokio::time::timeout(SOON, first.next())
            .await
            .expect("the stream ends rather than hanging")
            .is_none(),
        "a resolver returning None ends every session of that principal explicitly"
    );
    assert!(
        tokio::time::timeout(SOON, second.next())
            .await
            .expect("the stream ends rather than hanging")
            .is_none()
    );
    assert_eq!(render.live_sessions().await, 0);

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
