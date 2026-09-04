use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use conformance_service_engine::infra::TestDb;
use conformance_service_engine::sample::assignment::Assignment;
use conformance_service_engine::sample::note::Note;
use conformance_service_engine::sample::principal::{FailingPrincipalResolver, SampleRls};
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::delta::Delta;
use service_engine::impact::{Deps, Dims, Impact};
use service_engine::principal::Principal;
use service_engine::registry::RenderRegistry;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);
const QUIET: Duration = Duration::from_millis(300);

#[tokio::test]
async fn s22_a_principal_refresh_failure_ends_the_session_fail_closed() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let mut registry = RenderRegistry::new();
    registry.bind_noun::<Assignment>();
    registry.bind_noun::<Note>();
    registry.register_rls(SampleRls);
    registry.register_principal_resolver(FailingPrincipalResolver);
    registry
        .register_projector(SpyAssignments::new(spy.clone()))
        .expect("the projector registers");
    let engine = runtime(&pool, render_config("pod-refresh-fail"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    next_delta(&mut stream, SOON).await.expect("a Reset");

    let report = engine
        .render(vec![Impact::principal_facts(
            principal.id(),
            Deps::bit(0).unwrap(),
        )])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.ended, 1,
        "a principal whose refresh errors has its sessions ended explicitly, not served stale"
    );
    assert!(
        next_delta(&mut stream, SOON).await.is_none(),
        "the ended session's stream ends rather than going silent"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s22_a_failed_repair_never_resets_from_stale_state_and_ends_after_retries() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let switch = Arc::new(AtomicBool::new(false));
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()).with_fail_switch(switch.clone()))
        .expect("the projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-repair"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let opening = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert_eq!(opening.revision().get(), 1);
    assert!(matches!(opening, Delta::Reset { .. }));

    switch.store(true, Ordering::Relaxed);

    let mut ended = false;
    for _ in 0..12 {
        let report = engine
            .render(vec![resource(&subject, Dims::EMPTY)])
            .await
            .expect("the pass runs even while the slice keeps faulting");
        if report.ended > 0 {
            ended = true;
            break;
        }
        assert!(
            next_delta(&mut stream, QUIET).await.is_none(),
            "a session whose repair keeps failing is never handed a Reset built from its stale \
             last-sent view"
        );
    }
    assert!(
        ended,
        "a session that cannot be repaired after repeated attempts is ended, not left forever"
    );
    assert!(
        next_delta(&mut stream, SOON).await.is_none(),
        "the ended session's stream ends explicitly"
    );

    db.cleanup().await;
}
