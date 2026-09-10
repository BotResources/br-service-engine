use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::{AssignmentPage, PagedAssignments};
use service_engine::ViewProjector;
use service_engine::delta::Delta;
use service_engine::impact::{Deps, Dims, Impact};
use service_engine::principal::Principal;
use service_engine::session::{WindowParams, WindowSpec};
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

fn cursor(page: AssignmentPage) -> WindowParams {
    WindowParams::encode(&page).expect("the page cursor encodes")
}

#[tokio::test]
async fn s156_a_page_appends_older_keys_behind_the_live_window_without_a_reset() {
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
    let engine = runtime(&pool, render_config("pod-paging"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![WindowSpec::view::<PagedAssignments>(&AssignmentPage::head(2), false).unwrap()],
        ))
        .await
        .expect("the session attaches on the paged view");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    let mut head = assignment_ids(reset_views(&reset));
    head.sort();
    assert_eq!(
        head,
        vec![ids[4], ids[5]],
        "the live head is the newest page the query returned"
    );
    assert_eq!(reset.revision().get(), 1);

    let report = engine
        .page(
            &principal,
            stream.id(),
            PagedAssignments::NAME,
            cursor(AssignmentPage::before(ids[4], 2)),
        )
        .await
        .expect("the page request runs against the live session");
    assert_eq!(report.added, 2, "the older page appended two keys");
    assert_eq!(
        report.released, 0,
        "nothing fell off under a generous capacity"
    );
    assert_eq!(
        report.delivered, 2,
        "the two new keys are delivered as Upserts"
    );

    let first = next_delta(&mut stream, SOON).await.expect("an Upsert");
    let second = next_delta(&mut stream, SOON).await.expect("an Upsert");
    for (delta, expected) in [(&first, 2), (&second, 3)] {
        assert!(
            matches!(delta, Delta::Upsert { .. }),
            "scrolling back never triggers a Reset, got {delta:?}"
        );
        assert_eq!(
            delta.revision().get(),
            expected,
            "the revision stays contiguous across a page request"
        );
    }
    let mut paged: Vec<Uuid> = vec![
        assignment_ids(&[upserted(&first).clone()])[0],
        assignment_ids(&[upserted(&second).clone()])[0],
    ];
    paged.sort();
    assert_eq!(
        paged,
        vec![ids[2], ids[3]],
        "the page holds the two keys just before the head"
    );

    let deeper = engine
        .page(
            &principal,
            stream.id(),
            PagedAssignments::NAME,
            cursor(AssignmentPage::before(ids[2], 2)),
        )
        .await
        .expect("a second page request runs");
    assert_eq!(deeper.added, 2);
    let third = next_delta(&mut stream, SOON).await.expect("an Upsert");
    let fourth = next_delta(&mut stream, SOON).await.expect("an Upsert");
    assert!(matches!(third, Delta::Upsert { .. }) && matches!(fourth, Delta::Upsert { .. }));
    assert_eq!(
        fourth.revision().get(),
        5,
        "still contiguous, still no Reset"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s156_a_paged_history_survives_a_refresh_and_a_reconnect() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let mut ids = Vec::new();
    for n in 0..4 {
        ids.push(assignment(&pool, home, &format!("m{n}")).await);
    }

    let mut registry = registry();
    registry
        .register_projector(ViewProjector::new(PagedAssignments))
        .expect("the paged view registers");
    let engine = runtime(&pool, render_config("pod-paging-survive"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![WindowSpec::view::<PagedAssignments>(&AssignmentPage::head(1), false).unwrap()],
        ))
        .await
        .expect("the session attaches");
    next_delta(&mut stream, SOON).await.expect("a Reset");

    engine
        .page(
            &principal,
            stream.id(),
            PagedAssignments::NAME,
            cursor(AssignmentPage::before(ids[3], 2)),
        )
        .await
        .expect("a page appends the two rows behind the head");
    let _ = drain(&mut stream).await;

    let report = engine
        .render(vec![Impact::principal_facts(
            principal.id(),
            Deps::bit(0).unwrap(),
        )])
        .await
        .expect("the refresh pass runs");
    assert_eq!(
        report.ended, 0,
        "a refresh that keeps the membership ends no session"
    );
    let after_refresh = drain(&mut stream).await;
    assert!(
        !after_refresh
            .iter()
            .any(|delta| matches!(delta, Delta::Remove { .. })),
        "a refresh that keeps the membership drops no paged key: {after_refresh:?}"
    );

    retitle(&pool, ids[1], "edited after refresh").await;
    engine
        .render(vec![resource(&ids[1], Dims::ALL)])
        .await
        .expect("the edit pass runs");
    let edit = next_delta(&mut stream, SOON)
        .await
        .expect("an edit to a paged row still reaches the holder after the refresh");
    assert!(matches!(edit, Delta::Upsert { .. }), "got {edit:?}");
    assert_eq!(assignment_ids(&[upserted(&edit).clone()]), vec![ids[1]]);

    engine
        .resnapshot_all()
        .await
        .expect("the reconnect resnapshots every live session");
    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("the reconnect delivers a fresh Reset");
    assert!(matches!(reset, Delta::Reset { .. }), "got {reset:?}");
    let mut carried = assignment_ids(reset_views(&reset));
    carried.sort();
    let mut expected = vec![ids[1], ids[2], ids[3]];
    expected.sort();
    assert_eq!(
        carried, expected,
        "the reconnect Reset carries the live head and the appended page, not the head alone"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s156_an_edit_to_an_old_paged_row_reaches_the_viewer_who_holds_it() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let mut ids = Vec::new();
    for n in 0..4 {
        ids.push(assignment(&pool, home, &format!("m{n}")).await);
    }

    let mut registry = registry();
    registry
        .register_projector(ViewProjector::new(PagedAssignments))
        .expect("the paged view registers");
    let engine = runtime(&pool, render_config("pod-paging-edit"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![WindowSpec::view::<PagedAssignments>(&AssignmentPage::head(1), false).unwrap()],
        ))
        .await
        .expect("the session attaches");
    next_delta(&mut stream, SOON).await.expect("a Reset");

    engine
        .page(
            &principal,
            stream.id(),
            PagedAssignments::NAME,
            cursor(AssignmentPage::before(ids[3], 2)),
        )
        .await
        .expect("a page request appends older keys");
    let _ = drain(&mut stream).await;

    retitle(&pool, ids[1], "edited").await;
    let report = engine
        .render(vec![resource(&ids[1], Dims::ALL)])
        .await
        .expect("the pass runs");
    assert_eq!(report.deltas, 1, "an edit to a paged row is delivered once");
    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("the edit reaches the holder of the old row");
    assert!(matches!(delta, Delta::Upsert { .. }), "got {delta:?}");
    assert_eq!(assignment_ids(&[upserted(&delta).clone()]), vec![ids[1]]);

    db.cleanup().await;
}
