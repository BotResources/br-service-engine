use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::PerImpactAssignments;
use conformance_service_engine::sample::render::*;
use service_engine::ViewProjector;
use service_engine::delta::Delta;
use service_engine::impact::{Deps, Dims, Impact};
use service_engine::principal::Principal;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s160_a_per_impact_view_emits_the_cause_but_never_faults_on_a_causeless_impact() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let mut registry = registry();
    registry
        .register_projector(ViewProjector::new(PerImpactAssignments))
        .expect("the per-impact view registers on the bound noun");
    let engine = runtime(&pool, render_config("pod-per-impact"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(PerImpactAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    next_delta(&mut stream, SOON).await.expect("a Reset");

    retitle(&pool, subject, "renamed with a cause").await;
    let caused_report = engine
        .render(vec![caused(&subject, Dims::ALL, "edited")])
        .await
        .expect("the caused pass runs");
    assert!(caused_report.faults.is_empty());
    let with_cause = next_delta(&mut stream, SOON).await.expect("an Upsert");
    assert!(matches!(with_cause, Delta::Upsert { .. }));
    assert_eq!(
        upsert_cause(&with_cause).as_deref(),
        Some("edited"),
        "a caused impact still emits its cause per impact"
    );
    assert_eq!(with_cause.revision().get(), 2);

    retitle(&pool, subject, "renamed with no cause").await;
    let causeless_report = engine
        .render(vec![resource(&subject, Dims::ALL)])
        .await
        .expect("the causeless resource pass runs without faulting the per-impact view");
    assert!(
        causeless_report.faults.is_empty(),
        "a PerImpact projector must not fault on a causeless impact"
    );
    let coalesced = next_delta(&mut stream, SOON).await.expect("a coalesced Upsert");
    assert!(matches!(coalesced, Delta::Upsert { .. }), "got {coalesced:?}");
    assert_eq!(
        upsert_cause(&coalesced),
        None,
        "a causeless impact folds coalesced, so it carries no cause"
    );
    assert_eq!(
        coalesced.revision().get(),
        3,
        "the causeless delta advances the revision by exactly one, never a Reset"
    );

    let principal_report = engine
        .render(vec![Impact::principal_facts(
            principal.id(),
            Deps::bit(0).unwrap(),
        )])
        .await
        .expect("a causeless principal-facts impact runs");
    assert!(
        principal_report.faults.is_empty(),
        "a causeless PrincipalFactsChanged never faults the per-impact view either"
    );

    db.cleanup().await;
}
