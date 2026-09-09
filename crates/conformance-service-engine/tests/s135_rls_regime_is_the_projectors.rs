use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::error::AttachError;
use uuid::Uuid;

#[tokio::test]
async fn s135_a_window_asking_for_rls_on_a_non_rls_projector_is_refused() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;

    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(Spy::new()))
        .expect("the non-rls spy registers");
    let engine = runtime(&pool, render_config("pod-regime-a"), registry);

    let refusal = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, true)],
        ))
        .await
        .expect_err("a window contradicting the projector's regime is refused");
    assert!(
        matches!(
            refusal,
            AttachError::RlsRegimeMismatch {
                ref projector,
                declared: false,
                requested: true,
            } if projector == &SpyAssignments::NAME
        ),
        "the render regime is the projector's, not the call's, got {refusal:?}"
    );
    assert_eq!(engine.live_sessions().await, 0);

    db.cleanup().await;
}

#[tokio::test]
async fn s135_a_window_asking_for_no_rls_on_an_rls_projector_is_refused() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;

    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(Spy::new()).under_rls())
        .expect("the rls spy registers");
    let engine = runtime(&pool, render_config("pod-regime-b"), registry);

    let refusal = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect_err("a window contradicting the projector's regime is refused");
    assert!(
        matches!(
            refusal,
            AttachError::RlsRegimeMismatch {
                ref projector,
                declared: true,
                requested: false,
            } if projector == &SpyAssignments::NAME
        ),
        "an RLS projector cannot be attached as non-RLS by the call, got {refusal:?}"
    );
    assert_eq!(engine.live_sessions().await, 0);

    db.cleanup().await;
}
