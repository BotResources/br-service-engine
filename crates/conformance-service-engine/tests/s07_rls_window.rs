use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::principal::SamplePrincipalResolver;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::error::AttachError;
use service_engine::registry::RenderRegistry;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s07_rls_window() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let elsewhere = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let mine = assignment(&pool, home, "alpha").await;
    let theirs = assignment(&pool, elsewhere, "beta").await;

    let guarded = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(guarded.clone()))
        .expect("the spy projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-rls"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, true)],
        ))
        .await
        .expect("the RLS session attaches");
    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("a session opens with its Reset");
    assert_eq!(assignment_ids(reset_views(&reset)), vec![mine]);

    assert!(
        guarded.ever_loaded(mine),
        "the principal's own row is in Facts"
    );
    assert!(
        !guarded.ever_loaded(theirs),
        "a row invisible to the principal is never in Facts: the PerPrincipal load ran inside a \
         transaction the registered RlsApplier prepared"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s07_a_window_the_service_asked_to_run_under_rls_is_refused_without_an_applier() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    assignment(&pool, home, "alpha").await;

    let mut registry = RenderRegistry::new();
    registry.bind_noun::<conformance_service_engine::sample::assignment::Assignment>();
    registry.register_principal_resolver(SamplePrincipalResolver);
    registry
        .register_projector(SpyAssignments::new(Spy::new()))
        .expect("the spy projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-no-applier"), registry);

    let refusal = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, true)],
        ))
        .await
        .expect_err("an RLS window with no applier is refused");
    assert!(
        matches!(refusal, AttachError::MissingRlsApplier { ref projector } if projector == &SpyAssignments::NAME),
        "the refusal names the projector, got {refusal:?}"
    );
    assert_eq!(engine.live_sessions().await, 0, "no session exists");

    db.cleanup().await;
}

#[tokio::test]
async fn s07_a_window_attached_without_a_principal_resolver_is_refused() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;

    let mut registry = RenderRegistry::new();
    registry.bind_noun::<conformance_service_engine::sample::assignment::Assignment>();
    registry
        .register_projector(SpyAssignments::new(Spy::new()))
        .expect("the spy projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-no-resolver"), registry);

    let refusal = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect_err("a window with no principal resolver is refused");
    assert!(matches!(refusal, AttachError::MissingPrincipalResolver));

    db.cleanup().await;
}
