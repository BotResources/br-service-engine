use std::time::Duration;

use conformance_service_engine::infra::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::delta::Delta;
use service_engine::impact::{Dims, Impact};
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);
const QUIET: Duration = Duration::from_millis(200);

fn reset_title(delta: &Delta) -> String {
    let views = reset_views(delta);
    assert_eq!(views.len(), 1, "the window holds the one assignment");
    views[0].view.decode::<serde_json::Value>().unwrap()["title"]
        .as_str()
        .expect("a title")
        .to_string()
}

#[tokio::test]
async fn s196_a_reset_after_an_unread_delta_carries_the_committed_view_at_a_contiguous_revision() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()))
        .expect("the projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-reset-queued"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let opening = next_delta(&mut stream, SOON)
        .await
        .expect("a session opens with its Reset");
    assert_eq!(reset_title(&opening), "alpha");
    assert_eq!(opening.revision().get(), 1);

    retitle(&pool, subject, "beta").await;
    let queued = engine
        .render(vec![resource(&subject, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(
        queued.deltas, 1,
        "the rename is one Upsert the client leaves unread in its buffer"
    );

    retitle(&pool, subject, "gamma").await;
    let reset = engine
        .render(vec![Impact::projector_reset(SpyAssignments::NAME)])
        .await
        .expect("the projector reset pass runs");
    assert_eq!(reset.resets, 1, "the projector reset delivers one Reset");

    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("the reset replaces the unread Upsert on the wire");
    assert!(
        matches!(delta, Delta::Reset { .. }),
        "the queued Upsert is superseded by the Reset, never delivered stale first, got {delta:?}"
    );
    assert_eq!(
        reset_title(&delta),
        "gamma",
        "the Reset carries the committed view re-rendered in the reset pass, never the stale one \
         the unread delta held"
    );
    assert_eq!(
        delta.revision().get(),
        2,
        "the client read revision 1, so the Reset arrives at revision 2 and the sequence stays \
         contiguous even though the burned Upsert is never seen"
    );
    assert!(
        next_delta(&mut stream, QUIET).await.is_none(),
        "the Reset is the only frame the session sees after its opening one"
    );

    db.cleanup().await;
}
