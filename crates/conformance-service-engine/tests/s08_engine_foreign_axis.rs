#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{FOREIGN_NAMESPACE, Spy, SpyAssignments};
use engine_twin::{SILENCE, SOON, await_ready, spy_engine, stage};
use service_engine::impact::{ForeignKey, Impact};
use uuid::Uuid;

const CHANNEL: &str = "se_s08_engine";

#[tokio::test]
async fn s08_engine_a_foreign_fact_re_renders_only_the_keys_the_inverse_resolves() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let mirrored = assignment(&pool, home, "alpha").await;
    let untouched = assignment(&pool, home, "beta").await;

    let engine = spy_engine(
        &db,
        fabric,
        engine_config(CHANNEL, "pod-s08"),
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
    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");
    assert_eq!(reset_views(&reset).len(), 2);

    retitle(&pool, mirrored, "alpha mirrored").await;
    retitle(&pool, untouched, "beta mirrored").await;
    stage(
        &pool,
        transport.as_ref(),
        &[Impact::foreign(
            ForeignKey::new(FOREIGN_NAMESPACE, &mirrored.to_string()).expect("a valid foreign key"),
        )],
    )
    .await;
    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("a foreign fact reaches the keys the inverse resolves");
    assert_eq!(upserted(&delta).key.decode::<Uuid>().unwrap(), mirrored);
    assert!(
        next_delta(&mut stream, SILENCE).await.is_none(),
        "the key the inverse did not name is not re-rendered"
    );

    stage(
        &pool,
        transport.as_ref(),
        &[Impact::foreign(
            ForeignKey::new("identity.group", &mirrored.to_string()).expect("a valid foreign key"),
        )],
    )
    .await;
    assert!(
        next_delta(&mut stream, SILENCE).await.is_none(),
        "a namespace the inverse ignores reaches nothing"
    );

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
