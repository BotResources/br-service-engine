use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::VisibleAssignments;
use conformance_service_engine::sample::render::*;
use service_engine::ViewProjector;
use service_engine::delta::Delta;
use service_engine::impact::{Deps, Dims, Impact};
use service_engine::principal::Principal;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

fn subject_window() -> service_engine::session::WindowSpec {
    window(VisibleAssignments::NAME, false)
}

#[tokio::test]
async fn s132_a_row_that_leaves_the_cohort_is_removed_on_the_documented_view_surface() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let foreign = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let mut registry = registry();
    registry
        .register_projector(ViewProjector::new(VisibleAssignments))
        .expect("the documented view projector registers on the bound noun");
    let engine = runtime(&pool, render_config("pod-view-visibility"), registry);

    let mut stream = engine
        .attach(attach_request(&principal, vec![subject_window()]))
        .await
        .expect("the session attaches on the view surface");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert_eq!(
        assignment_ids(reset_views(&reset)),
        vec![subject],
        "the Fixed window populated by Visibility::window holds the visible row"
    );

    reassign(&pool, subject, foreign).await;
    let report = engine
        .render(vec![resource(&subject, Dims::ALL)])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 1,
        "a resource change that flips the row out of the cohort removes it, no principal change"
    );
    let delta = next_delta(&mut stream, SOON).await.expect("a Remove");
    assert!(
        matches!(delta, Delta::Remove { .. }),
        "project returns None for a now-invisible row on the documented surface, got {delta:?}"
    );
    assert_eq!(removed_key(&delta), subject);
    assert_eq!(delta.revision().get(), 2);

    reassign(&pool, subject, home).await;
    engine
        .render(vec![resource(&subject, Dims::ALL)])
        .await
        .expect("the pass runs");
    let back = next_delta(&mut stream, SOON)
        .await
        .expect("the row that regains its cohort reappears");
    assert!(
        matches!(back, Delta::Upsert { .. }),
        "a row that becomes visible again arrives as an Upsert, got {back:?}"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s132_a_fixed_window_is_repopulated_on_a_principal_change_both_directions() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let exile = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let mut registry = registry();
    registry
        .register_projector(ViewProjector::new(VisibleAssignments))
        .expect("the documented view projector registers on the bound noun");
    let engine = runtime(&pool, render_config("pod-view-membership"), registry);

    let mut stream = engine
        .attach(attach_request(&principal, vec![subject_window()]))
        .await
        .expect("the session attaches on the view surface");
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
        "losing the membership repopulates the Keys window and removes the row that left it"
    );
    let gone = next_delta(&mut stream, SOON).await.expect("a Remove");
    assert!(matches!(gone, Delta::Remove { .. }), "got {gone:?}");
    assert_eq!(removed_key(&gone), subject);
    assert_eq!(gone.revision().get(), 2);

    move_member(&pool, principal.id().as_uuid(), home).await;
    let report = engine
        .render(vec![Impact::principal_facts(
            principal.id(),
            Deps::bit(0).unwrap(),
        )])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 1,
        "regaining the membership repopulates the Keys window and the row arrives again"
    );
    let back = next_delta(&mut stream, SOON).await.expect("an Upsert");
    assert!(matches!(back, Delta::Upsert { .. }), "got {back:?}");
    assert_eq!(assignment_ids(&[upserted(&back).clone()]), vec![subject]);

    db.cleanup().await;
}
