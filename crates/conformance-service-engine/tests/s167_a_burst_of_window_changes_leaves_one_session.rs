use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::{AssignmentPage, PagedAssignments};
use futures_util::FutureExt;
use service_engine::ViewProjector;
use service_engine::impact::Dims;
use service_engine::session::WindowSpec;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);
const SILENCE: Duration = Duration::from_millis(300);

fn head(size: i64) -> WindowSpec {
    WindowSpec::view::<PagedAssignments>(&AssignmentPage::head(size), false)
        .expect("the window arguments encode")
}

#[tokio::test]
async fn s167_a_burst_of_window_changes_leaves_one_live_session_on_the_last_window() {
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
        .expect("the paged view registers on the bound noun");
    let engine = runtime(&pool, render_config("pod-burst"), registry);

    let mut current = None;
    for size in 1..=5 {
        let next = engine
            .attach(attach_request(&principal, vec![head(size)]))
            .await
            .expect("each window of the burst attaches");
        drop(current.replace(next));
    }
    let mut last = current.expect("the burst attached a last window");
    let reset = next_delta(&mut last, SOON).await.expect("a Reset");
    assert_eq!(reset.revision().get(), 1);
    assert_eq!(
        reset_views(&reset).len(),
        5,
        "the surviving session holds the last window of the burst"
    );
    assert!(next_delta(&mut last, SILENCE).await.is_none());

    retitle(&pool, ids[5], "edited").await;
    engine
        .render(vec![resource(&ids[5], Dims::ALL)])
        .await
        .expect("the pass runs");
    assert_eq!(
        engine.live_sessions().await,
        1,
        "every window the client left behind is dropped"
    );
    let upsert = next_delta(&mut last, SOON).await.expect("an Upsert");
    assert_eq!(upsert.revision().get(), 2);

    db.cleanup().await;
}

#[tokio::test]
async fn s167_a_window_left_while_still_attaching_leaves_no_pending_session() {
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
        .expect("the paged view registers on the bound noun");
    let engine = runtime(&pool, render_config("pod-burst-cancelled"), registry);

    for size in 1..=4 {
        let left = engine
            .attach(attach_request(&principal, vec![head(size)]))
            .now_or_never();
        assert!(
            left.is_none(),
            "the client moves on while the attach is still reading its window"
        );
    }
    assert_eq!(
        engine.pending_sessions().await,
        4,
        "the abandoned attaches were cut mid-snapshot"
    );

    let mut last = engine
        .attach(attach_request(&principal, vec![head(5)]))
        .await
        .expect("the last window of the burst attaches");
    let reset = next_delta(&mut last, SOON).await.expect("a Reset");
    assert_eq!(reset_views(&reset).len(), 5);

    retitle(&pool, ids[5], "edited").await;
    engine
        .render(vec![resource(&ids[5], Dims::ALL)])
        .await
        .expect("the pass runs");
    assert_eq!(
        engine.pending_sessions().await,
        0,
        "an attach the client abandoned is reaped by the next pass, not left for session_ttl"
    );
    assert_eq!(engine.live_sessions().await, 1);
    let upsert = next_delta(&mut last, SOON).await.expect("an Upsert");
    assert_eq!(upsert.revision().get(), 2);

    db.cleanup().await;
}
