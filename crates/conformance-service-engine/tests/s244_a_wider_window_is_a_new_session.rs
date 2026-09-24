use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::{AssignmentPage, PagedAssignments};
use service_engine::delta::Delta;
use service_engine::impact::Dims;
use service_engine::session::WindowSpec;
use service_engine::{ViewProjector, WindowSize};
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);
const SILENCE: Duration = Duration::from_millis(300);

fn window_size(size: u32) -> WindowSize {
    WindowSize::new(size).expect("a positive window size")
}

fn head(size: u32) -> WindowSpec {
    WindowSpec::view::<PagedAssignments>(&AssignmentPage::head(window_size(size)), false)
        .expect("the window arguments encode")
}

fn sorted(mut ids: Vec<Uuid>) -> Vec<Uuid> {
    ids.sort();
    ids
}

#[tokio::test]
async fn s244_a_wider_window_opens_a_new_session_with_one_reset_at_revision_one() {
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
    let engine = runtime(&pool, render_config("pod-wider"), registry);

    let mut narrow = engine
        .attach(attach_request(&principal, vec![head(2)]))
        .await
        .expect("the narrow window attaches");
    let first = next_delta(&mut narrow, SOON).await.expect("a Reset");
    assert_eq!(first.revision().get(), 1);
    assert_eq!(
        sorted(assignment_ids(reset_views(&first))),
        vec![ids[4], ids[5]],
        "the window is what populate returned for the subscription's arguments"
    );
    let narrow_id = narrow.id();
    drop(narrow);

    let mut wide = engine
        .attach(attach_request(&principal, vec![head(4)]))
        .await
        .expect("the wider window attaches as a new subscription");
    assert_ne!(
        wide.id(),
        narrow_id,
        "a window change is a new session, never the old one resized"
    );
    let reset = next_delta(&mut wide, SOON)
        .await
        .expect("the new session opens");
    assert!(
        matches!(reset, Delta::Reset { .. }),
        "the new window opens with a Reset, got {reset:?}"
    );
    assert_eq!(
        reset.revision().get(),
        1,
        "the new session counts its revisions from 1"
    );
    assert_eq!(
        sorted(assignment_ids(reset_views(&reset))),
        vec![ids[2], ids[3], ids[4], ids[5]],
        "the Reset carries the whole wider window, so the client needs no refetch"
    );
    assert!(
        next_delta(&mut wide, SILENCE).await.is_none(),
        "the switch costs exactly one Reset"
    );

    retitle(&pool, ids[2], "edited").await;
    engine
        .render(vec![resource(&ids[2], Dims::ALL)])
        .await
        .expect("the pass runs");
    assert_eq!(
        engine.live_sessions().await,
        1,
        "the pass dropped the narrow session the client released"
    );
    let upsert = next_delta(&mut wide, SOON)
        .await
        .expect("a key only the wider window holds changes");
    assert_eq!(upsert.revision().get(), 2);
    assert_eq!(
        assignment_ids(&[upserted(&upsert).clone()]),
        vec![ids[2]],
        "the wider window stays live on its own revision"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s244_a_page_behind_the_head_is_its_own_window_beside_the_live_head() {
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
    let engine = runtime(&pool, render_config("pod-page-behind"), registry);

    let mut live = engine
        .attach(attach_request(&principal, vec![head(2)]))
        .await
        .expect("the head attaches");
    let reset = next_delta(&mut live, SOON).await.expect("a Reset");
    assert_eq!(
        sorted(assignment_ids(reset_views(&reset))),
        vec![ids[4], ids[5]]
    );
    let behind = WindowSpec::view::<PagedAssignments>(
        &AssignmentPage::before(ids[4], window_size(2)),
        false,
    )
    .expect("the page arguments encode");
    let mut page = engine
        .attach(attach_request(&principal, vec![behind]))
        .await
        .expect("the page behind the head attaches as its own subscription");
    let reset = next_delta(&mut page, SOON).await.expect("a Reset");
    assert_eq!(reset.revision().get(), 1);
    assert_eq!(
        sorted(assignment_ids(reset_views(&reset))),
        vec![ids[2], ids[3]],
        "the page is what populate returned for its cursor"
    );

    let newest = assignment(&pool, home, "newest").await;
    engine
        .render(vec![resource(&newest, Dims::ALL)])
        .await
        .expect("the pass runs");
    let moved = drain(&mut live).await;
    assert!(
        moved
            .iter()
            .any(|delta| matches!(delta, Delta::Upsert { .. })
                && assignment_ids(&[upserted(delta).clone()]) == vec![newest]),
        "a new row enters the open head, got {moved:?}"
    );
    assert!(
        next_delta(&mut page, SILENCE).await.is_none(),
        "a page behind the head never takes a new row"
    );

    retitle(&pool, ids[2], "edited").await;
    engine
        .render(vec![resource(&ids[2], Dims::ALL)])
        .await
        .expect("the pass runs");
    let upsert = next_delta(&mut page, SOON)
        .await
        .expect("an edit to a row the page holds reaches it");
    assert_eq!(upsert.revision().get(), 2);
    assert_eq!(assignment_ids(&[upserted(&upsert).clone()]), vec![ids[2]]);
    assert_eq!(engine.live_sessions().await, 2);

    db.cleanup().await;
}
