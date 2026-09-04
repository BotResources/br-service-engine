use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{CohortMode, Spy, SpyAssignments};
use service_engine::impact::Dims;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s06_cohorts() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let one = member(&pool, Uuid::now_v7(), home).await;
    let two = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let shared = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(shared.clone()).with_cohort(CohortMode::PerTenant))
        .expect("the spy projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-cohorts"), registry);

    let mut first = engine
        .attach(attach_request(
            &one,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the first session attaches");
    let mut second = engine
        .attach(attach_request(
            &two,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the second session attaches");
    next_delta(&mut first, SOON).await.expect("a Reset");
    next_delta(&mut second, SOON).await.expect("a Reset");

    shared.reset();
    retitle(&pool, subject, "alpha renamed").await;
    let report = engine
        .render(vec![resource(&subject, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.cohorts, 1,
        "two principals whose cohort key is equal are one group"
    );
    assert_eq!(report.loads, 1, "a shared cohort is loaded once");
    assert_eq!(
        report.projections, 1,
        "a shared cohort is projected once per key"
    );
    assert_eq!(shared.loads(), 1);
    assert_eq!(shared.projects(), 1);
    assert_eq!(
        report.deltas, 2,
        "both sessions in the cohort are delivered"
    );

    let left = next_delta(&mut first, SOON).await.expect("an Upsert");
    let right = next_delta(&mut second, SOON).await.expect("an Upsert");
    assert_eq!(
        upserted(&left).view,
        upserted(&right).view,
        "a shared cohort delivers one identical view"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s06_two_principals_with_different_cohort_keys_are_loaded_and_projected_apart() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let one = member(&pool, Uuid::now_v7(), home).await;
    let two = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let split = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(
            SpyAssignments::new(split.clone()).with_cohort(CohortMode::PerPrincipal),
        )
        .expect("the spy projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-split"), registry);

    let mut first = engine
        .attach(attach_request(
            &one,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the first session attaches");
    let mut second = engine
        .attach(attach_request(
            &two,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the second session attaches");
    next_delta(&mut first, SOON).await.expect("a Reset");
    next_delta(&mut second, SOON).await.expect("a Reset");

    split.reset();
    retitle(&pool, subject, "alpha renamed").await;
    let report = engine
        .render(vec![resource(&subject, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(report.cohorts, 2);
    assert_eq!(report.loads, 2, "the default cohort shares nothing");
    assert_eq!(report.projections, 2);

    db.cleanup().await;
}
