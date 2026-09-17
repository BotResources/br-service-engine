use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::CohortIndexedAssignments;
use conformance_service_engine::sample::render::*;
use service_engine::ViewProjector;
use service_engine::delta::Delta;
use service_engine::impact::{Deps, Dims, Impact};
use service_engine::principal::Principal;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

fn subject_window() -> service_engine::session::WindowSpec {
    window(CohortIndexedAssignments::NAME, false)
}

#[tokio::test]
async fn s194_a_cohort_indexed_window_shows_exactly_the_callers_cohort_on_attach() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let elsewhere = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let mine = cohort_assignment(&pool, home, "mine").await;
    let _theirs = cohort_assignment(&pool, elsewhere, "theirs").await;

    let mut registry = registry();
    registry
        .register_projector(ViewProjector::new(CohortIndexedAssignments))
        .expect("the cohort-indexed view registers on the bound noun");
    let engine = runtime(&pool, render_config("pod-cohort-attach"), registry);

    let mut stream = engine
        .attach(attach_request(&principal, vec![subject_window()]))
        .await
        .expect("the session attaches on the cohort-indexed surface");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert_eq!(
        assignment_ids(reset_views(&reset)),
        vec![mine],
        "keys_in_cohorts returns only the caller's cohort row, read with one indexed query; \
         a row in another tenant's cohort is never populated"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s194_a_membership_grant_upserts_and_a_revoke_removes_on_the_cohort_indexed_window() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let limbo = Uuid::now_v7();
    let elsewhere = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let mine = cohort_assignment(&pool, home, "mine").await;
    let theirs = cohort_assignment(&pool, elsewhere, "theirs").await;

    let mut registry = registry();
    registry
        .register_projector(ViewProjector::new(CohortIndexedAssignments))
        .expect("the cohort-indexed view registers on the bound noun");
    let engine = runtime(&pool, render_config("pod-cohort-membership"), registry);

    let mut stream = engine
        .attach(attach_request(&principal, vec![subject_window()]))
        .await
        .expect("the session attaches");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert_eq!(assignment_ids(reset_views(&reset)), vec![mine]);

    move_member(&pool, principal.id().as_uuid(), limbo).await;
    engine
        .render(vec![Impact::principal_facts(
            principal.id(),
            Deps::bit(0).unwrap(),
        )])
        .await
        .expect("the pass runs");
    let gone = next_delta(&mut stream, SOON).await.expect("a Remove");
    assert!(
        matches!(gone, Delta::Remove { .. }),
        "revoking the membership removes the row the caller no longer sees, got {gone:?}"
    );
    assert_eq!(removed_key(&gone), mine);

    move_member(&pool, principal.id().as_uuid(), elsewhere).await;
    engine
        .render(vec![Impact::principal_facts(
            principal.id(),
            Deps::bit(0).unwrap(),
        )])
        .await
        .expect("the pass runs");
    let arrived = next_delta(&mut stream, SOON).await.expect("an Upsert");
    assert!(
        matches!(arrived, Delta::Upsert { .. }),
        "granting the membership upserts the row the caller now sees, got {arrived:?}"
    );
    assert_eq!(assignment_ids(&[upserted(&arrived).clone()]), vec![theirs]);

    db.cleanup().await;
}

#[tokio::test]
async fn s194_a_live_cohort_window_delivers_a_freshly_created_in_cohort_row() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let mine = cohort_assignment(&pool, home, "mine").await;

    let mut registry = registry();
    registry
        .register_projector(ViewProjector::new(CohortIndexedAssignments))
        .expect("the cohort-indexed view registers on the bound noun");
    let engine = runtime(&pool, render_config("pod-cohort-fresh"), registry);

    let mut stream = engine
        .attach(attach_request(&principal, vec![subject_window()]))
        .await
        .expect("the session attaches");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert_eq!(assignment_ids(reset_views(&reset)), vec![mine]);

    let fresh = cohort_assignment(&pool, home, "fresh").await;
    engine
        .render(vec![resource(&fresh, Dims::ALL)])
        .await
        .expect("the pass runs");
    let arrived = next_delta(&mut stream, SOON)
        .await
        .expect("the freshly created in-cohort row reaches the live session");
    assert!(
        matches!(arrived, Delta::Upsert { .. }),
        "a newly created in-cohort row arrives live on a derived Query window, got {arrived:?}"
    );
    assert_eq!(assignment_ids(&[upserted(&arrived).clone()]), vec![fresh]);

    db.cleanup().await;
}
