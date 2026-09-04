#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use engine_twin::{SILENCE, SOON, await_ready, spy_engine, stage};
use service_engine::delta::Delta;
use service_engine::impact::{Deps, Dims, Impact};
use service_engine::principal::Principal;
use uuid::Uuid;

const CHANNEL: &str = "se_s01_engine";

#[tokio::test]
async fn s01_engine_delta_table_drives_the_four_transitions_through_run() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let elsewhere = Uuid::now_v7();
    let exile = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let visible = assignment(&pool, home, "alpha").await;
    let hidden = assignment(&pool, elsewhere, "beta").await;

    let spy = Spy::new();
    let engine = spy_engine(
        &db,
        fabric,
        engine_config(CHANNEL, "pod-s01"),
        SpyAssignments::new(spy.clone()),
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
        .expect("the session attaches before the loop starts");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let opening = next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");
    assert_eq!(opening.revision().get(), 1);
    assert_eq!(assignment_ids(reset_views(&opening)), vec![visible]);

    reassign(&pool, hidden, home).await;
    stage(&pool, transport.as_ref(), &[resource(&hidden, Dims::EMPTY)]).await;
    let appeared = next_delta(&mut stream, SOON)
        .await
        .expect("a view the session has never seen arrives as an Upsert");
    assert_eq!(appeared.revision().get(), 2);
    assert_eq!(upserted(&appeared).key.decode::<Uuid>().unwrap(), hidden);

    retitle(&pool, visible, "alpha renamed").await;
    stage(
        &pool,
        transport.as_ref(),
        &[resource(&visible, Dims::EMPTY)],
    )
    .await;
    let changed = next_delta(&mut stream, SOON)
        .await
        .expect("a view whose content changed is an Upsert");
    assert_eq!(changed.revision().get(), 3);

    stage(
        &pool,
        transport.as_ref(),
        &[resource(&visible, Dims::EMPTY)],
    )
    .await;
    assert!(
        next_delta(&mut stream, SILENCE).await.is_none(),
        "a re-render that produced the same view emits nothing to the client"
    );

    move_member(&pool, principal.id().as_uuid(), exile).await;
    stage(
        &pool,
        transport.as_ref(),
        &[Impact::principal_facts(principal.id(), Deps::EMPTY)],
    )
    .await;
    let mut removed = Vec::new();
    for expected in 4..=5 {
        let delta = next_delta(&mut stream, SOON)
            .await
            .expect("a visibility loss caused by a principal fact change is a Remove");
        assert_eq!(delta.revision().get(), expected);
        assert!(matches!(delta, Delta::Remove { .. }));
        removed.push(removed_key(&delta));
    }
    removed.sort();
    let mut expected = vec![visible, hidden];
    expected.sort();
    assert_eq!(removed, expected, "every view the session held is removed");

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
