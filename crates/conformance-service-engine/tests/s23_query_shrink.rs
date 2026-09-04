use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments, WindowMode};
use service_engine::delta::Delta;
use service_engine::impact::Dims;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s23_a_key_that_leaves_populate_is_removed_even_when_project_would_still_render_it() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let other = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let alpha = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(
            SpyAssignments::new(spy.clone()).with_window(WindowMode::MembershipOnlyQuery),
        )
        .expect("the membership-only-query projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-shrink"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let opening = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert_eq!(
        assignment_ids(reset_views(&opening)),
        vec![alpha],
        "the window opens holding the one assignment populate reports for the tenant"
    );

    reassign(&pool, alpha, other).await;
    spy.reset();
    let report = engine
        .render(vec![resource(&alpha, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.populates, 1,
        "a ResourceChanged inside the Interest re-evaluates the authoritative membership"
    );

    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("the key that left populate's result is removed from the window");
    assert!(
        matches!(delta, Delta::Remove { .. }),
        "a key populate no longer reports is Removed, never re-upserted: {delta:?}"
    );
    assert_eq!(removed_key(&delta), alpha);
    assert_eq!(
        spy.projects(),
        0,
        "authoritative membership dropped the key before any render, so project is never called \
         for it — the Remove is the membership mechanism, not a project returning None"
    );
    assert_eq!(
        spy.loads(),
        0,
        "nothing is loaded for a key that left the window"
    );

    engine
        .resnapshot_all()
        .await
        .expect("a reconnect re-snapshots every session");
    let fresh = next_delta(&mut stream, SOON)
        .await
        .expect("the reconnect delivers a fresh Reset");
    assert!(
        reset_views(&fresh).is_empty(),
        "the disowned key never grows membership back: the fresh Reset carries nothing, so a \
         long-lived query session cannot accumulate keys it has lost, {fresh:?}"
    );

    db.cleanup().await;
}
