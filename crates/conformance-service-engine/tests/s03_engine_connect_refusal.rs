#[allow(dead_code)]
mod engine_twin;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use conformance_service_engine::sample::titles::TitleProjector;
use engine_twin::{SOON, await_ready, spy_engine, stage};
use service_engine::delta::Delta;
use service_engine::error::AttachError;
use service_engine::impact::Dims;
use uuid::Uuid;

const CHANNEL: &str = "se_s03_engine";

#[tokio::test]
async fn s03_engine_an_unassemblable_window_is_refused_and_the_running_engine_still_serves() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let known = assignment(&pool, home, "alpha").await;

    let mut engine = spy_engine(
        &db,
        fabric,
        engine_config(CHANNEL, "pod-s03"),
        SpyAssignments::new(Spy::new()).broken(),
    )
    .await;
    engine
        .register_projector(TitleProjector::new(Arc::new(AtomicUsize::new(0))))
        .expect("a second projector on the same noun registers");
    let readiness = engine.readiness();
    let transport = engine.transport_arc();
    let shutdown = engine.shutdown_handle();
    let render = engine.render();

    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let refused = render
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect_err("a window whose snapshot cannot be assembled refuses the connection");
    assert!(
        matches!(&refused, AttachError::Snapshot { projector, .. } if projector == &SpyAssignments::NAME),
        "the refusal names the window that could not be assembled, got {refused}"
    );
    assert_eq!(
        render.live_sessions().await,
        0,
        "a refused connection leaves no session behind"
    );

    let mut stream = render
        .attach(attach_request(
            &principal,
            vec![window(TitleProjector::NAME, false)],
        ))
        .await
        .expect("a window that does assemble still attaches on the running engine");
    let opening = next_delta(&mut stream, SOON)
        .await
        .expect("a session that opens opens with a Reset, never with an error");
    assert!(matches!(opening, Delta::Reset { .. }));
    assert_eq!(opening.revision().get(), 1);
    assert_eq!(reset_views(&opening).len(), 1);

    retitle(&pool, known, "alpha renamed").await;
    stage(&pool, transport.as_ref(), &[resource(&known, Dims::EMPTY)]).await;
    let upsert = next_delta(&mut stream, SOON)
        .await
        .expect("the healthy window keeps rendering after a refused connection");
    assert_eq!(upsert.revision().get(), 2);
    assert_eq!(upserted(&upsert).projector, TitleProjector::NAME);

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
