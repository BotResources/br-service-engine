use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use conformance_service_engine::sample::titles::TitleProjector;
use service_engine::delta::Delta;
use service_engine::impact::Dims;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s05_emission() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()))
        .expect("the spy projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-coalesced"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    next_delta(&mut stream, SOON)
        .await
        .expect("a session opens with its Reset");

    retitle(&pool, subject, "alpha renamed").await;
    let report = engine
        .render(vec![
            resource(&subject, Dims::EMPTY),
            resource(&subject, Dims::EMPTY),
            resource(&subject, Dims::EMPTY),
        ])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 1,
        "three impacts on one key inside one window are one Upsert under Coalesced"
    );
    assert_eq!(report.loads, 1, "one load per cohort, not one per impact");
    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("the coalesced Upsert reaches the session");
    assert_eq!(delta.revision().get(), 2);
    assert_eq!(
        upsert_cause(&delta),
        None,
        "a coalesced delta drops the causes it folded"
    );
    assert!(
        next_delta(&mut stream, Duration::from_millis(100))
            .await
            .is_none()
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s05_per_impact_emits_one_delta_per_cause_and_refuses_an_impact_that_carries_none() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()).per_impact())
        .expect("the spy projector registers on a bound noun");
    registry
        .register_projector(TitleProjector::new(Arc::new(AtomicUsize::new(0))))
        .expect("a coalescing projector registers on the same noun");
    let engine = runtime(&pool, render_config("pod-per-impact"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    next_delta(&mut stream, SOON)
        .await
        .expect("a session opens with its Reset");

    let report = engine
        .render(vec![
            caused(&subject, Dims::EMPTY, "renamed"),
            caused(&subject, Dims::EMPTY, "closed"),
            caused(&subject, Dims::EMPTY, "reopened"),
        ])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 3,
        "PerImpact yields one delta per impact and skips the equality check"
    );
    let mut causes = Vec::new();
    for expected in 2..=4 {
        let delta = next_delta(&mut stream, SOON)
            .await
            .expect("every PerImpact delta reaches the session");
        assert_eq!(delta.revision().get(), expected);
        causes.push(upsert_cause(&delta).expect("a PerImpact delta carries its cause"));
    }
    assert_eq!(causes, vec!["renamed", "closed", "reopened"]);

    let mut coalescing = engine
        .attach(attach_request(
            &principal,
            vec![window(TitleProjector::NAME, false)],
        ))
        .await
        .expect("a second session attaches on a coalescing projector");
    next_delta(&mut coalescing, SOON)
        .await
        .expect("the second session opens with its Reset");

    retitle(&pool, subject, "alpha once more").await;
    let report = engine
        .render(vec![resource(&subject, Dims::EMPTY)])
        .await
        .expect("a refusal on one session does not abort the pass");
    assert_eq!(
        report.faults.len(),
        1,
        "PerImpact without a cause faults exactly the session that asked for it"
    );
    let fault = &report.faults[0];
    assert!(
        fault.reason.contains("PerImpact") && fault.reason.contains(SpyAssignments::NAME.as_str()),
        "the fault names the projector that could not emit, got {}",
        fault.reason
    );
    assert!(
        fault.repaired,
        "a faulted session is re-snapshotted inside the pass instead of being left stale"
    );
    assert_eq!(
        report.deltas, 1,
        "the session on another projector still receives its delta"
    );
    let delta = next_delta(&mut coalescing, SOON)
        .await
        .expect("the coalescing session is untouched by the other session's refusal");
    assert!(matches!(delta, Delta::Upsert { .. }));
    let repair = next_delta(&mut stream, SOON)
        .await
        .expect("the faulted session is reset, never silently starved");
    assert!(
        matches!(repair, Delta::Reset { .. }),
        "the repair of a faulted session is a Reset, got {repair:?}"
    );

    db.cleanup().await;
}
