use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::CohortAssignments;
use conformance_service_engine::sample::render::*;
use service_engine::ViewProjector;
use service_engine::impact::Dims;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s158_one_load_per_cohort_per_frame_whatever_the_number_of_viewers() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let tenant_a = Uuid::now_v7();
    let tenant_b = Uuid::now_v7();
    let subject_a = assignment(&pool, tenant_a, "a").await;
    let subject_b = assignment(&pool, tenant_b, "b").await;

    let a1 = member(&pool, Uuid::now_v7(), tenant_a).await;
    let a2 = member(&pool, Uuid::now_v7(), tenant_a).await;
    let a3 = member(&pool, Uuid::now_v7(), tenant_a).await;
    let b1 = member(&pool, Uuid::now_v7(), tenant_b).await;
    let b2 = member(&pool, Uuid::now_v7(), tenant_b).await;

    let mut registry = registry();
    registry
        .register_projector(ViewProjector::new(CohortAssignments))
        .expect("the cohort view registers on the bound noun");
    let engine = runtime(&pool, render_config("pod-view-cohort"), registry);

    let mut streams = Vec::new();
    for principal in [&a1, &a2, &a3, &b1, &b2] {
        let mut stream = engine
            .attach(attach_request(
                principal,
                vec![window(CohortAssignments::NAME, false)],
            ))
            .await
            .expect("the session attaches on the cohort view");
        next_delta(&mut stream, SOON).await.expect("a Reset");
        streams.push(stream);
    }

    retitle(&pool, subject_a, "a renamed").await;
    retitle(&pool, subject_b, "b renamed").await;
    let report = engine
        .render(vec![resource(&subject_a, Dims::ALL), resource(&subject_b, Dims::ALL)])
        .await
        .expect("the pass runs");

    assert_eq!(
        report.cohorts, 2,
        "three viewers in tenant A and two in tenant B are two cohorts on the view surface"
    );
    assert_eq!(
        report.loads, 2,
        "each cohort loads once per frame, not once per session"
    );
    assert_eq!(
        report.projections, 2,
        "each cohort projects its one dirty key once"
    );
    assert_eq!(report.deltas, 5, "all five sessions receive their Upsert");

    let mut first_a = None;
    for (index, stream) in streams.iter_mut().enumerate() {
        let delta = next_delta(stream, SOON).await.expect("an Upsert");
        if index < 3 {
            let view = upserted(&delta).view.clone();
            match &first_a {
                None => first_a = Some(view),
                Some(shared) => assert_eq!(
                    &view, shared,
                    "the three tenant-A viewers share one identical rendered view"
                ),
            }
        }
    }

    db.cleanup().await;
}
