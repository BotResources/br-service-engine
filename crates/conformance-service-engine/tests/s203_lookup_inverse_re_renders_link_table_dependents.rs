use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::{LINK_NAMESPACE, LinkedAssignments, link_assignment};
use service_engine::impact::{ForeignKey, Impact};
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

fn foreign(key: &str) -> Impact {
    Impact::foreign(ForeignKey::new(LINK_NAMESPACE, key).expect("a valid foreign key"))
}

#[tokio::test]
async fn s203_a_lookup_inverse_re_renders_only_the_link_table_dependents() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let alpha = assignment(&pool, home, "alpha").await;
    let beta = assignment(&pool, home, "beta").await;
    let gamma = assignment(&pool, home, "gamma").await;

    let entry = Uuid::now_v7().to_string();
    link_assignment(&pool, alpha, &entry).await;
    link_assignment(&pool, beta, &entry).await;

    let mut registry = registry();
    registry
        .register_projector(LinkedAssignments)
        .expect("the link-table view registers on the bound noun");
    let engine = runtime(&pool, render_config("pod-lookup"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(LinkedAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert_eq!(
        reset_views(&reset).len(),
        3,
        "every tenant row is populated"
    );

    retitle(&pool, alpha, "alpha moved").await;
    retitle(&pool, beta, "beta moved").await;
    retitle(&pool, gamma, "gamma moved").await;

    let report = engine
        .render(vec![foreign(&entry)])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 2,
        "the lookup resolves exactly the two link-table dependents, and no others re-render"
    );

    let mut touched = vec![
        upserted(&next_delta(&mut stream, SOON).await.expect("first upsert"))
            .key
            .decode::<Uuid>()
            .unwrap(),
        upserted(&next_delta(&mut stream, SOON).await.expect("second upsert"))
            .key
            .decode::<Uuid>()
            .unwrap(),
    ];
    touched.sort();
    let mut expected = vec![alpha, beta];
    expected.sort();
    assert_eq!(
        touched, expected,
        "the two dependents of the foreign key re-render"
    );
    assert!(
        next_delta(&mut stream, Duration::from_millis(100))
            .await
            .is_none(),
        "a row the lookup did not name is not re-rendered by the foreign impact"
    );

    db.cleanup().await;
}
