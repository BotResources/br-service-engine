use std::sync::Arc;
use std::time::Duration;

use conformance_service_engine::infra::TestDb;
use conformance_service_engine::infra::listener::{engine_config, pool_named};
use conformance_service_engine::sample::render::{
    assignment, attach_request, member, next_delta, registry, runtime, window,
};
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::boot::REASON_LISTEN_FAILED;
use service_engine::delta::Delta;
use service_engine::housekeeping::beat::Beat;
use service_engine::housekeeping::mirror::MirrorSupervisor;
use service_engine::housekeeping::ready::ReadinessAssembly;
use service_engine::stop::Stop;
use service_engine::transport::{ImpactTransport, PgListenNotify};
use service_engine::{Readiness, ReadinessHandle};
use sqlx::PgPool;
use uuid::Uuid;

const CHANNEL: &str = "se_s133_impact";
const LISTENER: &str = "se_s133_listener";
const SOON: Duration = Duration::from_secs(20);
const TICK_PAUSE: Duration = Duration::from_millis(25);
const OVER_THRESHOLD: f64 = 0.9;
const UNDER_THRESHOLD: f64 = 0.0;

#[tokio::test]
async fn s133_queue_usage_past_the_threshold_takes_the_pod_down_then_up_with_a_reset() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    assignment(&pool, home, "alpha").await;

    let config = engine_config(CHANNEL, "pod-brake");
    let transport = Arc::new(
        PgListenNotify::connect(pool_named(&db, db.app_role(), LISTENER).await, &config)
            .await
            .expect("the listener is established"),
    );

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()))
        .expect("the projector registers on a bound noun");
    let engine = runtime(&pool, config.clone(), registry);

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
            .expect("a session opens with its Reset")
            .revision()
            .get(),
        1
    );

    let readiness = ReadinessHandle::ready();
    let assembly = ReadinessAssembly::new(readiness.clone(), MirrorSupervisor::new().health())
        .with_listener(transport.listener_health());
    let mut beat = Beat::from_config(&config)
        .expect("a beat from the engine config")
        .with_transport(transport.clone())
        .with_readiness(assembly);

    let stop_render = Stop::new();
    let running = tokio::spawn(engine.clone().run(transport.listen(), stop_render.clone()));

    beat.tick(&pool).await;
    assert_eq!(
        readiness.snapshot(),
        Readiness::Ready,
        "a pod whose notification queue is quiet stays in rotation"
    );

    transport.force_queue_usage(OVER_THRESHOLD);
    tick_until(&mut beat, &pool, || {
        matches!(readiness.snapshot(), Readiness::NotReady { reason } if reason == REASON_LISTEN_FAILED)
    })
    .await;

    transport.force_queue_usage(UNDER_THRESHOLD);
    tick_until(&mut beat, &pool, || {
        readiness.snapshot() == Readiness::Ready
    })
    .await;

    let reset = await_reset(&mut stream).await;
    assert!(
        matches!(reset, Delta::Reset { .. }),
        "closing and reopening the listener resets the pod's sessions, got {reset:?}"
    );
    assert!(
        engine.metrics().transport_reconnects >= 1,
        "the brake's close and reconnect surfaced as a transport reconnect"
    );

    stop_render.stop();
    running.await.expect("the render task stops");
    db.cleanup().await;
}

async fn tick_until(beat: &mut Beat, pool: &PgPool, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + SOON;
    loop {
        beat.tick(pool).await;
        if done() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the readiness gate never reached the awaited state under the queue brake"
        );
        tokio::time::sleep(TICK_PAUSE).await;
    }
}

async fn await_reset(stream: &mut service_engine::session::SessionStream) -> Delta {
    let deadline = tokio::time::Instant::now() + SOON;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let delta = next_delta(stream, remaining)
            .await
            .expect("the reconnect delivers a Reset");
        if matches!(delta, Delta::Reset { .. }) {
            return delta;
        }
    }
}
