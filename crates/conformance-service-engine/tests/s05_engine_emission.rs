#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use engine_twin::{SILENCE, SOON, await_ready, spy_engine, stage};
use service_engine::impact::Dims;
use uuid::Uuid;

const COALESCED: &str = "se_s05_coalesced";
const PER_IMPACT: &str = "se_s05_per_impact";

#[tokio::test]
async fn s05_engine_three_impacts_on_one_key_in_one_window_are_one_coalesced_upsert() {
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
        engine_config(COALESCED, "pod-s05c"),
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
    stage(
        &pool,
        transport.as_ref(),
        &[
            resource(&subject, Dims::EMPTY),
            resource(&subject, Dims::EMPTY),
            resource(&subject, Dims::EMPTY),
        ],
    )
    .await;
    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("three impacts on one key in one frame fold to one Upsert");
    assert_eq!(delta.revision().get(), 2);
    assert_eq!(
        upsert_cause(&delta),
        None,
        "a coalesced delta drops the causes it folded"
    );
    assert!(
        next_delta(&mut stream, SILENCE).await.is_none(),
        "three impacts inside one window must not yield three deltas"
    );

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s05_engine_per_impact_emits_one_delta_per_cause() {
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
        engine_config(PER_IMPACT, "pod-s05p"),
        SpyAssignments::new(Spy::new()).per_impact(),
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

    stage(
        &pool,
        transport.as_ref(),
        &[
            caused(&subject, Dims::EMPTY, "renamed"),
            caused(&subject, Dims::EMPTY, "closed"),
            caused(&subject, Dims::EMPTY, "reopened"),
        ],
    )
    .await;
    let mut causes = Vec::new();
    for expected in 2..=4 {
        let delta = next_delta(&mut stream, SOON)
            .await
            .expect("every PerImpact delta reaches the session");
        assert_eq!(delta.revision().get(), expected);
        causes.push(upsert_cause(&delta).expect("a PerImpact delta carries its cause"));
    }
    assert_eq!(causes, vec!["renamed", "closed", "reopened"]);

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
