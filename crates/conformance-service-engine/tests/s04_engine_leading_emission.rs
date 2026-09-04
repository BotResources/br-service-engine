#[allow(dead_code)]
mod engine_twin;

use std::time::{Duration, Instant};

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use engine_twin::{SOON, await_ready, spy_engine, stage};
use service_engine::impact::Dims;
use uuid::Uuid;

const CHANNEL: &str = "se_s04_engine";
const WINDOW: Duration = Duration::from_millis(800);

#[tokio::test]
async fn s04_engine_a_lone_impact_after_idle_is_delivered_before_the_window_elapses() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let engine = spy_engine(
        &db,
        fabric,
        engine_config(CHANNEL, "pod-s04").with_window(WINDOW),
        SpyAssignments::new(Spy::new()),
    )
    .await;
    let readiness = engine.readiness();
    let transport = engine.transport_arc();
    let shutdown = engine.shutdown_handle();

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;
    next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");

    retitle(&pool, subject, "alpha renamed").await;
    let staged = Instant::now();
    stage(
        &pool,
        transport.as_ref(),
        &[resource(&subject, Dims::EMPTY)],
    )
    .await;
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

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
