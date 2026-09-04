use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use futures_util::StreamExt;
use service_engine::delta::Delta;
use service_engine::impact::{Deps, Impact};
use service_engine::principal::Principal;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s22_principal_refresh() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let exile = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()))
        .expect("the spy projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-refresh"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, true)],
        ))
        .await
        .expect("the RLS session attaches");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert_eq!(assignment_ids(reset_views(&reset)), vec![subject]);

    move_member(&pool, principal.id().as_uuid(), exile).await;
    let report = engine
        .render(vec![Impact::principal_facts(
            principal.id(),
            Deps::bit(0).unwrap(),
        )])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 1,
        "revoking a local fact drops the row on the same pass, with no resource mutation"
    );
    let delta = next_delta(&mut stream, SOON).await.expect("a Remove");
    assert!(matches!(delta, Delta::Remove { .. }));
    assert_eq!(removed_key(&delta), subject);
    assert_eq!(delta.revision().get(), 2);

    db.cleanup().await;
}

#[tokio::test]
async fn s22_a_resolver_that_no_longer_returns_the_principal_ends_every_session_it_holds() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()))
        .expect("the spy projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-revoked"), registry);

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
    next_delta(&mut first, SOON).await.expect("a Reset");
    next_delta(&mut second, SOON).await.expect("a Reset");
    assert_eq!(engine.live_sessions().await, 2);

    forget_member(&pool, principal.id().as_uuid()).await;
    let report = engine
        .render(vec![Impact::principal_facts(principal.id(), Deps::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.ended, 2,
        "a resolver returning None ends every session of that principal explicitly"
    );
    assert_eq!(engine.live_sessions().await, 0);
    assert!(
        tokio::time::timeout(SOON, first.next())
            .await
            .expect("the stream ends rather than hanging")
            .is_none(),
        "an ended session's stream ends, it does not go silent"
    );
    assert!(
        tokio::time::timeout(SOON, second.next())
            .await
            .expect("the stream ends rather than hanging")
            .is_none()
    );

    db.cleanup().await;
}
