use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::delta::Delta;
use service_engine::impact::{Dims, Impact};
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(5);
const SILENCE: Duration = Duration::from_millis(300);
const BUFFER: usize = 4;

fn impacts(ids: &[Uuid]) -> Vec<Impact> {
    ids.iter().map(|id| resource(id, Dims::EMPTY)).collect()
}

#[tokio::test]
async fn s12_lag() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let mut ids = Vec::new();
    for n in 0..8 {
        ids.push(assignment(&pool, home, &format!("row {n}")).await);
    }

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()))
        .expect("the projector registers on a bound noun");
    let engine = runtime(
        &pool,
        render_config("pod-lag").with_session_buffer(BUFFER),
        registry,
    );

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
    assert_eq!(opening.revision().get(), 1);
    assert_eq!(reset_views(&opening).len(), 8);

    for (n, id) in ids.iter().enumerate() {
        retitle(&pool, *id, &format!("renamed {n}")).await;
    }
    let report = engine
        .render(impacts(&ids))
        .await
        .expect("the pass runs even though the session cannot take its deltas");
    assert_eq!(
        report.lagged, 1,
        "a session whose buffer cannot take the pass is a lag, counted, not a silent drop"
    );
    assert_eq!(report.resets, 1);
    assert_eq!(
        report.deltas, 0,
        "a lagging session receives the Reset, never a prefix of the deltas it could not take"
    );
    assert_eq!(report.discarded, 8);
    assert_eq!(engine.metrics().lag_resets, 1);

    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("a lagging session is reset, never ended");
    assert_eq!(
        reset.revision().get(),
        2,
        "the Reset follows the revision the client last read"
    );
    let views = reset_views(&reset);
    assert_eq!(views.len(), 8);
    assert!(
        views.iter().all(
            |view| view.view.decode::<serde_json::Value>().unwrap()["title"]
                .as_str()
                .expect("a title")
                .starts_with("renamed")
        ),
        "the Reset carries last_sent, which is exactly the state the client is expected to hold"
    );
    assert!(
        next_delta(&mut stream, SILENCE).await.is_none(),
        "the Reset replaced the buffer, it was not appended to a queue of stale deltas"
    );

    retitle(&pool, ids[0], "after the reset").await;
    let report = engine
        .render(impacts(&ids[..1]))
        .await
        .expect("the pass runs");
    assert_eq!(report.deltas, 1);
    assert_eq!(report.lagged, 0);
    let resumed = next_delta(&mut stream, SOON)
        .await
        .expect("delivery resumes after a lag");
    assert_eq!(
        resumed.revision().get(),
        3,
        "the revision is contiguous once the session is caught up"
    );
    assert!(matches!(resumed, Delta::Upsert { .. }));

    engine.shutdown().await;
    assert!(
        next_delta(&mut stream, SOON).await.is_none(),
        "the stream ends explicitly on shutdown, and only then"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s12_a_reset_that_replaces_unread_deltas_keeps_the_revision_contiguous() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let mut ids = Vec::new();
    for n in 0..8 {
        ids.push(assignment(&pool, home, &format!("row {n}")).await);
    }

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()))
        .expect("the projector registers on a bound noun");
    let engine = runtime(
        &pool,
        render_config("pod-unread").with_session_buffer(BUFFER),
        registry,
    );

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    assert_eq!(
        next_delta(&mut stream, SOON)
            .await
            .expect("the opening Reset")
            .revision()
            .get(),
        1
    );

    retitle(&pool, ids[0], "first").await;
    retitle(&pool, ids[1], "second").await;
    let report = engine
        .render(impacts(&ids[..2]))
        .await
        .expect("the pass runs");
    assert_eq!(report.deltas, 2, "two deltas fit and are delivered");
    assert_eq!(
        stream.buffered(),
        2,
        "the client has read neither of them yet"
    );

    for id in &ids {
        retitle(&pool, *id, "again").await;
    }
    let report = engine
        .render(impacts(&ids))
        .await
        .expect("the pass runs on a session that is already behind");
    assert_eq!(report.lagged, 1);
    assert_eq!(report.resets, 1);

    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("the lagging session is reset");
    assert!(matches!(reset, Delta::Reset { .. }));
    assert_eq!(
        reset.revision().get(),
        2,
        "revisions burned by deltas the client never read are handed back, so the frame it \
         actually reads follows the frame it actually read"
    );
    assert!(next_delta(&mut stream, SILENCE).await.is_none());

    db.cleanup().await;
}
