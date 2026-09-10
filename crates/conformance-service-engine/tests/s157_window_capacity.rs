use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::{AssignmentPage, PagedAssignments};
use service_engine::ViewProjector;
use service_engine::delta::Delta;
use service_engine::impact::Dims;
use service_engine::session::{WindowParams, WindowSpec};
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

fn cursor(page: AssignmentPage) -> WindowParams {
    WindowParams::encode(&page).expect("the page cursor encodes")
}

#[tokio::test]
async fn s157_capacity_releases_the_oldest_page_silently_and_keeps_the_live_head() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let mut ids = Vec::new();
    for n in 0..6 {
        ids.push(assignment(&pool, home, &format!("m{n}")).await);
    }

    let mut registry = registry();
    registry
        .register_projector(ViewProjector::new(PagedAssignments))
        .expect("the paged view registers");
    let engine = runtime(
        &pool,
        render_config("pod-capacity").with_window_capacity(4),
        registry,
    );

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![WindowSpec::view::<PagedAssignments>(&AssignmentPage::head(2), false).unwrap()],
        ))
        .await
        .expect("the session attaches");
    next_delta(&mut stream, SOON).await.expect("a Reset");

    let first = engine
        .page(
            stream.id(),
            PagedAssignments::NAME,
            cursor(AssignmentPage::before(ids[4], 2)),
        )
        .await
        .expect("the first page appends the keys before the head");
    assert_eq!(first.added, 2);
    assert_eq!(
        first.released, 0,
        "the window is exactly at capacity, nothing falls off"
    );
    let _ = drain(&mut stream).await;

    let second = engine
        .page(
            stream.id(),
            PagedAssignments::NAME,
            cursor(AssignmentPage::before(ids[2], 2)),
        )
        .await
        .expect("the second page pushes the window past capacity");
    assert_eq!(second.added, 2, "the deepest page appends two keys");
    assert_eq!(
        second.released, 2,
        "past window_capacity the oldest page is released"
    );
    assert_eq!(second.delivered, 2, "only the new keys are delivered");

    let deltas = drain(&mut stream).await;
    assert_eq!(
        deltas.len(),
        2,
        "the released page produces no delta of its own"
    );
    for delta in &deltas {
        assert!(
            matches!(delta, Delta::Upsert { .. }),
            "a page released for capacity never emits a Remove, got {delta:?}"
        );
    }
    let mut arrived: Vec<Uuid> = deltas
        .iter()
        .map(|delta| assignment_ids(&[upserted(delta).clone()])[0])
        .collect();
    arrived.sort();
    assert_eq!(
        arrived,
        vec![ids[0], ids[1]],
        "the deepest page's keys arrived"
    );

    retitle(&pool, ids[2], "edited-released").await;
    let released = engine
        .render(vec![resource(&ids[2], Dims::ALL)])
        .await
        .expect("the pass runs");
    assert_eq!(
        released.deltas, 0,
        "a change to a released key is not delivered; the client dropped the page"
    );

    retitle(&pool, ids[5], "edited-head").await;
    let head = engine
        .render(vec![resource(&ids[5], Dims::ALL)])
        .await
        .expect("the pass runs");
    assert_eq!(head.deltas, 1, "the live head is retained across eviction");

    db.cleanup().await;
}
