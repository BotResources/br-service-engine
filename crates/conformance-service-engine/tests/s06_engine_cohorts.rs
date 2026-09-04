#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{CohortMode, Spy, SpyAssignments};
use engine_twin::{SOON, await_ready, spy_engine, stage};
use service_engine::impact::Dims;
use uuid::Uuid;

const CHANNEL: &str = "se_s06_engine";

#[tokio::test]
async fn s06_engine_one_cohort_loads_once_and_delivers_identical_views() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let one = member(&pool, Uuid::now_v7(), home).await;
    let two = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let shared = Spy::new();
    let engine = spy_engine(
        &db,
        fabric,
        engine_config(CHANNEL, "pod-s06"),
        SpyAssignments::new(shared.clone()).with_cohort(CohortMode::PerTenant),
    )
    .await;
    let readiness = engine.readiness();
    let transport = engine.transport_arc();
    let shutdown = engine.shutdown_handle();

    let mut first = engine
        .attach(attach_request(
            &one,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the first session attaches");
    let mut second = engine
        .attach(attach_request(
            &two,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the second session attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;
    next_delta(&mut first, SOON).await.expect("first Reset");
    next_delta(&mut second, SOON).await.expect("second Reset");

    shared.reset();
    retitle(&pool, subject, "alpha renamed").await;
    stage(
        &pool,
        transport.as_ref(),
        &[resource(&subject, Dims::EMPTY)],
    )
    .await;

    let left = next_delta(&mut first, SOON)
        .await
        .expect("an Upsert to the first session");
    let right = next_delta(&mut second, SOON)
        .await
        .expect("an Upsert to the second session");
    assert_eq!(
        upserted(&left).view,
        upserted(&right).view,
        "two principals whose cohort key is equal receive one identical view"
    );
    assert_eq!(
        shared.loads(),
        1,
        "a shared cohort is loaded once for the whole pass, not once per session"
    );
    assert_eq!(
        shared.projects(),
        1,
        "a shared cohort is projected once per key"
    );

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
