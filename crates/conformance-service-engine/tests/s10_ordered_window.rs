use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments, WindowMode};
use service_engine::delta::Delta;
use service_engine::impact::Dims;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);
const HEAD: i64 = 2;

#[tokio::test]
async fn s10_ordered_window() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let top = assignment(&pool, home, "m-first").await;
    let second = assignment(&pool, home, "l-second").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(
            SpyAssignments::new(spy.clone()).with_window(WindowMode::OrderedHead(HEAD)),
        )
        .expect("the spy projector registers on a bound noun");
    let engine = runtime(
        &pool,
        render_config("pod-ordered").with_reset_threshold(2),
        registry,
    );

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    let mut opening = assignment_ids(reset_views(&reset));
    opening.sort();
    let mut expected = vec![top, second];
    expected.sort();
    assert_eq!(opening, expected);

    let sorts_in = assignment(&pool, home, "z-newest").await;
    let report = engine
        .render(vec![resource(&sorts_in, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.populates, 1,
        "an open_head window is re-evaluated by the impacts that touch its noun"
    );
    let mut delivered = Vec::new();
    for _ in 0..report.deltas {
        delivered.push(
            next_delta(&mut stream, SOON)
                .await
                .expect("the head change reaches the session"),
        );
    }
    let upserts: Vec<Uuid> = delivered
        .iter()
        .filter(|d| matches!(d, Delta::Upsert { .. }))
        .map(|d| upserted(d).key.decode::<Uuid>().unwrap())
        .collect();
    let removes: Vec<Uuid> = delivered
        .iter()
        .filter(|d| matches!(d, Delta::Remove { .. }))
        .map(removed_key)
        .collect();
    assert_eq!(
        upserts,
        vec![sorts_in],
        "a key that sorts into the head is upserted"
    );
    assert_eq!(
        removes,
        vec![second],
        "the key it pushed out of the head is removed"
    );

    let sorts_out = assignment(&pool, home, "a-oldest").await;
    let report = engine
        .render(vec![resource(&sorts_out, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 0,
        "a key that does not sort into the head is ignored"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s10_a_membership_change_beyond_the_reset_threshold_is_a_reset() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    assignment(&pool, home, "a-one").await;
    assignment(&pool, home, "b-two").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(
            SpyAssignments::new(spy.clone()).with_window(WindowMode::OrderedHead(HEAD)),
        )
        .expect("the spy projector registers on a bound noun");
    let engine = runtime(
        &pool,
        render_config("pod-churn").with_reset_threshold(1),
        registry,
    );

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    next_delta(&mut stream, SOON).await.expect("a Reset");

    assignment(&pool, home, "y-new").await;
    let churner = assignment(&pool, home, "z-newest").await;
    let report = engine
        .render(vec![resource(&churner, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.resets, 1,
        "a membership change beyond reset_threshold keys is a Reset, not a run of deltas"
    );
    let frame = next_delta(&mut stream, SOON)
        .await
        .expect("the Reset reaches the session");
    assert!(matches!(frame, Delta::Reset { .. }));
    assert_eq!(frame.revision().get(), 2);
    assert_eq!(reset_views(&frame).len(), 2);

    db.cleanup().await;
}

#[tokio::test]
async fn s10_a_head_member_that_vanishes_alone_is_removed_and_forgotten() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let stays = assignment(&pool, home, "m-first").await;
    let leaves = assignment(&pool, home, "l-second").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(
            SpyAssignments::new(spy.clone()).with_window(WindowMode::OrderedHead(HEAD)),
        )
        .expect("the spy projector registers on a bound noun");
    let engine = runtime(
        &pool,
        render_config("pod-vanish").with_reset_threshold(1),
        registry,
    );

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert_eq!(reset_views(&reset).len(), 2);

    delete_assignment(&pool, leaves).await;
    let report = engine
        .render(vec![resource(&leaves, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 1,
        "a head member that leaves with nothing taking its place is still one delta"
    );
    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("losing the last visibility of a key reaches the session");
    assert_eq!(
        removed_key(&delta),
        leaves,
        "a key that vanishes from the only window holding it is removed"
    );
    assert_eq!(delta.revision().get(), 2);

    let first_new = assignment(&pool, home, "y-new").await;
    let second_new = assignment(&pool, home, "z-newest").await;
    let report = engine
        .render(vec![resource(&second_new, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.resets, 1,
        "a membership change beyond reset_threshold keys is a Reset"
    );
    let frame = next_delta(&mut stream, SOON)
        .await
        .expect("the Reset reaches the session");
    let mut carried = assignment_ids(reset_views(&frame));
    carried.sort();
    let mut expected = vec![first_new, second_new];
    expected.sort();
    assert_eq!(
        carried, expected,
        "the Reset carries the head as it stands, never a key the session was told to remove"
    );
    assert!(
        !carried.contains(&leaves) && !carried.contains(&stays),
        "a removed key is forgotten, not resurrected by the next Reset"
    );

    db.cleanup().await;
}
