use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::{AssignmentPage, AssignmentView, PagedAssignments};
use service_engine::delta::{Delta, ErasedView};
use service_engine::impact::Dims;
use service_engine::session::WindowSpec;
use service_engine::{ViewProjector, WindowSize};
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

fn window_size(size: u32) -> WindowSize {
    WindowSize::new(size).expect("a positive window size")
}

fn head(size: u32) -> WindowSpec {
    WindowSpec::view::<PagedAssignments>(&AssignmentPage::head(window_size(size)), false)
        .expect("the window arguments encode")
}

fn title_in(views: &[ErasedView], key: Uuid) -> Option<String> {
    views
        .iter()
        .find(|view| view.key.decode::<Uuid>().expect("a key decodes") == key)
        .map(|view| {
            view.view
                .decode::<AssignmentView>()
                .expect("an assignment view decodes")
                .title
        })
}

fn final_title(deltas: &[Delta], key: Uuid) -> Option<String> {
    deltas.iter().rev().find_map(|delta| match delta {
        Delta::Reset { views, .. } => title_in(views, key),
        Delta::Upsert { view, .. } => title_in(std::slice::from_ref(view), key),
        _ => None,
    })
}

#[tokio::test]
async fn s247_a_write_committed_between_two_windows_is_in_the_new_reset() {
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
        .expect("the paged view registers on the bound noun");
    let engine = runtime(&pool, render_config("pod-switch-gap"), registry);

    let mut before = engine
        .attach(attach_request(&principal, vec![head(2)]))
        .await
        .expect("the first window attaches");
    next_delta(&mut before, SOON).await.expect("a Reset");
    drop(before);

    retitle(&pool, ids[3], "edited in the gap").await;
    let created = assignment(&pool, home, "created in the gap").await;
    engine
        .render(vec![
            resource(&ids[3], Dims::ALL),
            resource(&created, Dims::ALL),
        ])
        .await
        .expect("the pass runs while no window is live");

    let mut after = engine
        .attach(attach_request(&principal, vec![head(3)]))
        .await
        .expect("the next window attaches");
    let reset = next_delta(&mut after, SOON).await.expect("a Reset");
    let views = reset_views(&reset);
    assert_eq!(
        title_in(views, ids[3]).as_deref(),
        Some("edited in the gap"),
        "an edit committed while the client switched windows is in the new Reset"
    );
    assert_eq!(
        title_in(views, created).as_deref(),
        Some("created in the gap"),
        "a row created while the client switched windows is in the new Reset"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s247_a_write_racing_the_new_attach_settles_on_the_committed_view() {
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
        .expect("the paged view registers on the bound noun");
    let engine = runtime(&pool, render_config("pod-switch-race"), registry);
    let key = ids[3];

    let mut current = None;
    for round in 0..8 {
        let fresh = format!("v{round}");
        let (attached, passed) = tokio::join!(
            engine.attach(attach_request(&principal, vec![head(2 + round % 2)])),
            async {
                retitle(&pool, key, &fresh).await;
                engine.render(vec![resource(&key, Dims::ALL)]).await
            }
        );
        passed.expect("the concurrent edit pass runs");
        let mut stream = attached.expect("the next window attaches");
        drop(current.take());
        let mut deltas = Vec::new();
        deltas.push(next_delta(&mut stream, SOON).await.expect("a Reset"));
        deltas.extend(drain(&mut stream).await);
        assert_eq!(
            final_title(&deltas, key).as_deref(),
            Some(fresh.as_str()),
            "a write that races the switch reaches the new window, never lost to it"
        );
        current = Some(stream);
    }

    db.cleanup().await;
}
