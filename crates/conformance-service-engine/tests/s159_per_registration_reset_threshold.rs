use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::{AssignmentProjector, ThresholdAssignments};
use service_engine::ViewProjector;
use service_engine::delta::Delta;
use service_engine::impact::{Deps, Impact};
use service_engine::principal::Principal;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s159_a_registration_threshold_resets_where_the_global_default_still_diffs() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let mut ids = Vec::new();
    for n in 0..3 {
        ids.push(assignment(&pool, home, &format!("m{n}")).await);
    }

    let mut registry = registry();
    registry
        .register_projector(ViewProjector::new(ThresholdAssignments))
        .expect("the low-threshold view registers");
    registry
        .register_projector(AssignmentProjector)
        .expect("the default-threshold projector registers");
    let engine = runtime(&pool, render_config("pod-threshold"), registry);

    let mut low = engine
        .attach(attach_request(
            &principal,
            vec![window(ThresholdAssignments::NAME, false)],
        ))
        .await
        .expect("the low-threshold session attaches");
    let mut default = engine
        .attach(attach_request(
            &principal,
            vec![window(AssignmentProjector::NAME, false)],
        ))
        .await
        .expect("the default-threshold session attaches");
    assert_eq!(reset_views(&next_delta(&mut low, SOON).await.unwrap()).len(), 3);
    assert_eq!(reset_views(&next_delta(&mut default, SOON).await.unwrap()).len(), 3);

    delete_assignment(&pool, ids[0]).await;
    delete_assignment(&pool, ids[1]).await;
    engine
        .render(vec![Impact::principal_facts(
            principal.id(),
            Deps::bit(0).unwrap(),
        )])
        .await
        .expect("the repopulating pass runs");

    let low_delta = next_delta(&mut low, SOON)
        .await
        .expect("the low-threshold window churned past its registration threshold");
    match low_delta {
        Delta::Reset { views, .. } => assert_eq!(
            views.len(),
            1,
            "a churn of two exceeds RESET_THRESHOLD = 1, so a fresh Reset is sent"
        ),
        other => panic!("the low-threshold window resets, got {other:?}"),
    }

    let first = next_delta(&mut default, SOON).await.expect("a Remove");
    let second = next_delta(&mut default, SOON).await.expect("a Remove");
    assert!(
        matches!(first, Delta::Remove { .. }) && matches!(second, Delta::Remove { .. }),
        "the same churn under the global default stays under threshold and diffs, got {first:?} / {second:?}"
    );

    db.cleanup().await;
}
