use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{FOREIGN_NAMESPACE, Spy, SpyAssignments};
use service_engine::impact::{ForeignKey, Impact};
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s08_foreign_axis() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let mirrored = assignment(&pool, home, "alpha").await;
    let untouched = assignment(&pool, home, "beta").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()))
        .expect("the spy projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-foreign"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert_eq!(reset_views(&reset).len(), 2);

    retitle(&pool, mirrored, "alpha mirrored").await;
    retitle(&pool, untouched, "beta mirrored").await;
    let report = engine
        .render(vec![Impact::foreign(
            ForeignKey::new(FOREIGN_NAMESPACE, &mirrored.to_string()).expect("a valid foreign key"),
        )])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 1,
        "a foreign fact reaches the keys the projector's inverse resolves, and no others"
    );
    let delta = next_delta(&mut stream, SOON).await.expect("an Upsert");
    assert_eq!(upserted(&delta).key.decode::<Uuid>().unwrap(), mirrored);
    assert!(
        next_delta(&mut stream, Duration::from_millis(100))
            .await
            .is_none(),
        "the key the inverse did not name is not re-rendered"
    );

    let report = engine
        .render(vec![Impact::foreign(
            ForeignKey::new("identity.group", &mirrored.to_string()).expect("a valid foreign key"),
        )])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 0,
        "a namespace the inverse ignores reaches nothing"
    );

    db.cleanup().await;
}
