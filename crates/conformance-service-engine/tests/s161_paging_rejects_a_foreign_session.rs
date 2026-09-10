use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::{AssignmentPage, PagedAssignments};
use service_engine::ViewProjector;
use service_engine::error::EngineError;
use service_engine::session::{WindowParams, WindowSpec};
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

fn cursor(page: AssignmentPage) -> WindowParams {
    WindowParams::encode(&page).expect("the page cursor encodes")
}

#[tokio::test]
async fn s161_a_page_request_for_a_session_the_caller_does_not_own_is_refused() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let alice = member(&pool, Uuid::now_v7(), home).await;
    let mallory = member(&pool, Uuid::now_v7(), home).await;
    let mut ids = Vec::new();
    for n in 0..4 {
        ids.push(assignment(&pool, home, &format!("m{n}")).await);
    }

    let mut registry = registry();
    registry
        .register_projector(ViewProjector::new(PagedAssignments))
        .expect("the paged view registers");
    let engine = runtime(&pool, render_config("pod-paging-owner"), registry);

    let mut alice_stream = engine
        .attach(attach_request(
            &alice,
            vec![WindowSpec::view::<PagedAssignments>(&AssignmentPage::head(1), false).unwrap()],
        ))
        .await
        .expect("alice attaches her own session");
    next_delta(&mut alice_stream, SOON)
        .await
        .expect("alice's Reset");

    let mut mallory_stream = engine
        .attach(attach_request(
            &mallory,
            vec![WindowSpec::view::<PagedAssignments>(&AssignmentPage::head(1), false).unwrap()],
        ))
        .await
        .expect("mallory attaches her own session on the same pod");
    next_delta(&mut mallory_stream, SOON)
        .await
        .expect("mallory's Reset");

    let refused = engine
        .page(
            &mallory,
            alice_stream.id(),
            PagedAssignments::NAME,
            cursor(AssignmentPage::before(ids[3], 2)),
        )
        .await;
    assert!(
        matches!(refused, Err(EngineError::NoLiveSession { .. })),
        "paging a live session the caller does not own is refused, got {refused:?}"
    );

    let leaked = drain(&mut alice_stream).await;
    assert!(
        leaked.is_empty(),
        "the foreign page request grew and delivered nothing on the victim's wire, got {leaked:?}"
    );

    let owned = engine
        .page(
            &mallory,
            mallory_stream.id(),
            PagedAssignments::NAME,
            cursor(AssignmentPage::before(ids[3], 2)),
        )
        .await
        .expect("the caller still pages a session she owns");
    assert!(
        owned.added >= 1,
        "the ownership check is not a blanket denial: a caller pages her own window"
    );

    db.cleanup().await;
}
