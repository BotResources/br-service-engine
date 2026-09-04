use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::delta::Delta;
use service_engine::impact::{Deps, Dims, Impact};
use service_engine::principal::Principal;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s01_delta_table() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let elsewhere = Uuid::now_v7();
    let exile = Uuid::now_v7();

    let principal = member(&pool, Uuid::now_v7(), home).await;
    let visible = assignment(&pool, home, "alpha").await;
    let hidden = assignment(&pool, elsewhere, "beta").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()))
        .expect("the spy projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-delta"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");

    let first = next_delta(&mut stream, SOON)
        .await
        .expect("a session opens with its Reset");
    assert_eq!(first.revision().get(), 1);
    assert_eq!(assignment_ids(reset_views(&first)), vec![visible]);

    reassign(&pool, hidden, home).await;
    let report = engine
        .render(vec![resource(&hidden, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(report.deltas, 1);
    let appeared = next_delta(&mut stream, SOON)
        .await
        .expect("a view a session has never seen is an Upsert");
    assert_eq!(appeared.revision().get(), 2);
    assert_eq!(
        upserted(&appeared).key.decode::<Uuid>().unwrap(),
        hidden,
        "the newly visible assignment is the one upserted"
    );

    retitle(&pool, visible, "alpha renamed").await;
    let report = engine
        .render(vec![resource(&visible, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(report.deltas, 1);
    let changed = next_delta(&mut stream, SOON)
        .await
        .expect("a view whose content changed is an Upsert");
    assert_eq!(changed.revision().get(), 3);

    let report = engine
        .render(vec![resource(&visible, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 0,
        "a re-render that produced the same view emits nothing"
    );
    assert!(
        next_delta(&mut stream, Duration::from_millis(100))
            .await
            .is_none(),
        "an unchanged view must not reach the client"
    );

    move_member(&pool, principal.id().as_uuid(), exile).await;
    let report = engine
        .render(vec![Impact::principal_facts(principal.id(), Deps::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 2,
        "losing visibility removes every view the session held, with no resource mutation"
    );
    let mut removed = Vec::new();
    for expected in 4..=5 {
        let delta = next_delta(&mut stream, SOON)
            .await
            .expect("a visibility loss is a Remove");
        assert_eq!(delta.revision().get(), expected);
        assert!(matches!(delta, Delta::Remove { .. }));
        removed.push(removed_key(&delta));
    }
    removed.sort();
    let mut expected = vec![visible, hidden];
    expected.sort();
    assert_eq!(removed, expected);

    db.cleanup().await;
}
