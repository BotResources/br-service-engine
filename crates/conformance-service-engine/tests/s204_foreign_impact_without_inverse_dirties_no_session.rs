use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::LinkedAssignments;
use conformance_service_engine::sample::render::*;
use service_engine::impact::{ForeignKey, Impact};
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s204_a_foreign_impact_a_view_declares_no_inverse_for_dirties_no_session() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let alpha = assignment(&pool, home, "alpha").await;

    let mut registry = registry();
    registry
        .register_projector(LinkedAssignments)
        .expect("the link-table view registers on the bound noun");
    let engine = runtime(&pool, render_config("pod-no-inverse"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(LinkedAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert_eq!(assignment_ids(reset_views(&reset)), vec![alpha]);

    retitle(&pool, alpha, "alpha moved").await;

    let report = engine
        .render(vec![Impact::foreign(
            ForeignKey::new("identity.group", &Uuid::now_v7().to_string())
                .expect("a valid foreign key"),
        )])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 0,
        "a foreign impact the view declares no inverse for dirties no session"
    );
    assert_eq!(report.loads, 0, "no key is loaded");
    assert_eq!(report.projections, 0, "no key is projected");
    assert!(
        next_delta(&mut stream, Duration::from_millis(100))
            .await
            .is_none(),
        "no delta reaches the live session; the retitle is not delivered"
    );

    db.cleanup().await;
}
