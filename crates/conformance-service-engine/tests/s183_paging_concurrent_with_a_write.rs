use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::{AssignmentPage, AssignmentView, PagedAssignments};
use service_engine::ViewProjector;
use service_engine::delta::Delta;
use service_engine::impact::Dims;
use service_engine::session::{WindowParams, WindowSpec};
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

fn cursor(page: AssignmentPage) -> WindowParams {
    WindowParams::encode(&page).expect("the page cursor encodes")
}

fn last_title_for(deltas: &[Delta], key: Uuid) -> Option<String> {
    deltas
        .iter()
        .rev()
        .filter(|delta| matches!(delta, Delta::Upsert { .. }))
        .filter(|delta| {
            upserted(delta)
                .key
                .decode::<Uuid>()
                .expect("an assignment key decodes")
                == key
        })
        .map(|delta| {
            upserted(delta)
                .view
                .decode::<AssignmentView>()
                .expect("an assignment view decodes")
                .title
        })
        .next()
}

#[tokio::test]
async fn s183_a_key_paged_in_during_a_concurrent_write_never_settles_on_the_stale_view() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let mut ids = Vec::new();
    for n in 0..12 {
        ids.push(assignment(&pool, home, &format!("m{n}")).await);
    }

    let mut registry = registry();
    registry
        .register_projector(ViewProjector::new(PagedAssignments))
        .expect("the paged view registers");
    let engine = runtime(&pool, render_config("pod-paging-race"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![WindowSpec::view::<PagedAssignments>(&AssignmentPage::head(1), false).unwrap()],
        ))
        .await
        .expect("the session attaches on the paged view");
    next_delta(&mut stream, SOON).await.expect("a Reset");

    for i in (1..11).rev() {
        let key = ids[i];
        let fresh = format!("v2-{i}");
        let (paged, passed) = tokio::join!(
            engine.page(
                &principal,
                stream.id(),
                PagedAssignments::NAME,
                cursor(AssignmentPage::before(ids[i + 1], 1)),
            ),
            async {
                retitle(&pool, key, &fresh).await;
                engine.render(vec![resource(&key, Dims::ALL)]).await
            }
        );
        paged.expect("the page request runs against the live session");
        passed.expect("the concurrent edit pass runs");

        let deltas = drain(&mut stream).await;
        assert_eq!(
            last_title_for(&deltas, key).as_deref(),
            Some(fresh.as_str()),
            "the client's final view of a key paged in during a concurrent write is the committed \
             value, never a stale page render that lost the race to the pass",
        );
    }

    db.cleanup().await;
}
