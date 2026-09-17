use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::render::{
    ImpactFeed, assignment, attach_request, member, next_delta, registry, render_config, resource,
    runtime, window,
};
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::delta::Delta;
use service_engine::impact::Dims;
use service_engine::stop::Stop;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(15);
const CHANNEL_CAPACITY: usize = 2;
const BURST: usize = 64;

#[tokio::test]
async fn s132_a_burst_larger_than_the_in_process_channel_resets_every_live_session() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let watched = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()))
        .expect("the projector registers on a bound noun");
    let config = render_config("pod-overflow")
        .with_listener_channel_capacity(CHANNEL_CAPACITY)
        .with_window(Duration::from_millis(20));
    let engine = runtime(&pool, config, registry);

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

    let (feed, source) = ImpactFeed::new();
    for _ in 0..BURST {
        feed.impacts(vec![resource(&watched, Dims::EMPTY)]);
    }

    let stop = Stop::new();
    let running = tokio::spawn(engine.clone().run(source, stop.clone()));

    let reset = await_reset(&mut stream).await;
    assert!(
        matches!(reset, Delta::Reset { .. }),
        "a burst that overflows the bounded in-process channel is a detectable loss, so every \
         live session is re-snapshotted rather than left with a silent gap, got {reset:?}"
    );
    assert!(
        engine.metrics().transport_reconnects >= 1,
        "the channel overflow surfaced as a transport reconnect"
    );
    assert!(
        engine.metrics().resets >= 1,
        "the reconnect reset the session"
    );

    drop(feed);
    stop.stop();
    running.await.expect("the render task stops");
    db.cleanup().await;
}

async fn await_reset(stream: &mut service_engine::session::SessionStream) -> Delta {
    let deadline = tokio::time::Instant::now() + SOON;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let delta = next_delta(stream, remaining)
            .await
            .expect("the session keeps delivering until a Reset is seen");
        if matches!(delta, Delta::Reset { .. }) {
            return delta;
        }
    }
}
