#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments, WindowMode};
use engine_twin::{SILENCE, SOON, await_ready, spy_engine, stage};
use service_engine::delta::Delta;
use service_engine::impact::Dims;
use uuid::Uuid;

const CHANNEL: &str = "se_s10_engine";
const HEAD: i64 = 2;

#[tokio::test]
async fn s10_engine_a_key_that_sorts_into_the_head_is_upserted_and_pushes_one_out() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let top = assignment(&pool, home, "m-first").await;
    let second = assignment(&pool, home, "l-second").await;

    let engine = spy_engine(
        &db,
        fabric,
        engine_config(CHANNEL, "pod-s10").with_reset_threshold(2),
        SpyAssignments::new(Spy::new()).with_window(WindowMode::OrderedHead(HEAD)),
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
    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");
    let mut opening = assignment_ids(reset_views(&reset));
    opening.sort();
    let mut expected = vec![top, second];
    expected.sort();
    assert_eq!(opening, expected);

    let sorts_in = assignment(&pool, home, "z-newest").await;
    stage(
        &pool,
        transport.as_ref(),
        &[resource(&sorts_in, Dims::EMPTY)],
    )
    .await;
    let mut upserts = Vec::new();
    let mut removes = Vec::new();
    for _ in 0..2 {
        match next_delta(&mut stream, SOON)
            .await
            .expect("the head change reaches the session")
        {
            d @ Delta::Upsert { .. } => upserts.push(upserted(&d).key.decode::<Uuid>().unwrap()),
            d @ Delta::Remove { .. } => removes.push(removed_key(&d)),
            other => panic!("unexpected delta {other:?}"),
        }
    }
    assert_eq!(
        upserts,
        vec![sorts_in],
        "a key that sorts into the head is upserted"
    );
    assert_eq!(
        removes,
        vec![second],
        "the key it pushed out of the head is removed"
    );

    let sorts_out = assignment(&pool, home, "a-oldest").await;
    stage(
        &pool,
        transport.as_ref(),
        &[resource(&sorts_out, Dims::EMPTY)],
    )
    .await;
    assert!(
        next_delta(&mut stream, SILENCE).await.is_none(),
        "a key that does not sort into the head is ignored"
    );

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
