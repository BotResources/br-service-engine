#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::gate::Gate;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments, WindowMode};
use engine_twin::{SILENCE, SOON, await_ready, spy_engine, stage};
use service_engine::delta::Delta;
use service_engine::impact::Dims;
use uuid::Uuid;

const CHANNEL: &str = "se_s02_engine";

#[tokio::test]
async fn s02_engine_an_impact_committed_during_the_snapshot_is_replayed_exactly_once() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let known = assignment(&pool, home, "alpha").await;

    let gate = Gate::new();
    let engine = spy_engine(
        &db,
        fabric,
        engine_config(CHANNEL, "pod-s02"),
        SpyAssignments::new(Spy::new())
            .with_window(WindowMode::OrderedHead(50))
            .gated(gate.clone()),
    )
    .await;
    let readiness = engine.readiness();
    let transport = engine.transport_arc();
    let shutdown = engine.shutdown_handle();
    let render = engine.render();

    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let connecting = {
        let render = render.clone();
        let request = attach_request(&principal, vec![window(SpyAssignments::NAME, false)]);
        tokio::spawn(async move { render.attach(request).await })
    };

    gate.wait_until_inside().await;
    let late = assignment(&pool, home, "beta").await;
    stage(&pool, transport.as_ref(), &[resource(&late, Dims::EMPTY)]).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    gate.release();

    let mut stream = connecting
        .await
        .expect("the attach task completes")
        .expect("the session attaches");

    let first = next_delta(&mut stream, SOON)
        .await
        .expect("a session opens with its Reset");
    assert!(
        matches!(first, Delta::Reset { .. }),
        "the Reset is the first frame even though a pass ran while the snapshot assembled, got {first:?}"
    );
    assert_eq!(first.revision().get(), 1);
    assert_eq!(
        assignment_ids(reset_views(&first)),
        vec![known],
        "the snapshot holds what the window read, not the row staged after it"
    );

    let held = next_delta(&mut stream, SOON)
        .await
        .expect("the impact committed during the snapshot is replayed, never discarded");
    assert_eq!(held.revision().get(), 2);
    assert_eq!(
        upserted(&held).key.decode::<Uuid>().unwrap(),
        late,
        "the row committed during the snapshot reaches the session exactly once"
    );
    assert!(
        next_delta(&mut stream, SILENCE).await.is_none(),
        "the held impact is replayed once, not twice"
    );

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
