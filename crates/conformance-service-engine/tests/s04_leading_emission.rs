use std::sync::Arc;
use std::time::{Duration, Instant};

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::impact::Dims;
use tokio::sync::Notify;
use uuid::Uuid;

const WINDOW: Duration = Duration::from_millis(800);
const SOON: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s04_leading_emission() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()))
        .expect("the spy projector registers on a bound noun");
    let engine = runtime(
        &pool,
        render_config("pod-leading").with_window(WINDOW),
        registry,
    );

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    next_delta(&mut stream, SOON)
        .await
        .expect("a session opens with its Reset");

    let (feed, events) = ImpactFeed::new();
    let shutdown = Arc::new(Notify::new());
    let loop_handle = tokio::spawn(engine.clone().run(events, shutdown.clone()));

    retitle(&pool, subject, "alpha renamed").await;
    let staged = Instant::now();
    feed.impacts(vec![resource(&subject, Dims::EMPTY)]);

    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("the first impact after idle is rendered in a pass of its own");
    let waited = staged.elapsed();
    assert_eq!(delta.revision().get(), 2);
    assert!(
        waited < WINDOW,
        "a lone impact after idle must be delivered before the coalescing window elapses, \
         waited {waited:?} for a {WINDOW:?} window"
    );

    retitle(&pool, subject, "alpha renamed twice").await;
    feed.impacts(vec![resource(&subject, Dims::EMPTY)]);
    retitle(&pool, subject, "alpha renamed thrice").await;
    feed.impacts(vec![resource(&subject, Dims::EMPTY)]);
    let coalesced = next_delta(&mut stream, SOON)
        .await
        .expect("the impacts inside the window fold into one pass");
    assert_eq!(coalesced.revision().get(), 3);
    assert!(
        next_delta(&mut stream, Duration::from_millis(200))
            .await
            .is_none(),
        "two impacts inside one window must not yield two deltas"
    );

    shutdown.notify_waiters();
    let _ = tokio::time::timeout(SOON, loop_handle).await;
    db.cleanup().await;
}
