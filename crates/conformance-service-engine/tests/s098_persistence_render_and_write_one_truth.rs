#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_persistence_engine;
use conformance_service_engine::sample::counter::{
    BumpFull, FullCounterProjector, FullCounterStore,
};
use conformance_service_engine::sample::render::{
    attach_request, member, next_delta, reset_views, upserted, window,
};
use engine_twin::{SOON, await_ready};
use serde::Deserialize;
use service_engine::persistence::Persistence;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct CounterDto {
    total: i64,
}

#[tokio::test]
async fn s098_the_render_side_and_the_write_side_read_the_same_committed_full_eda_store() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;

    let engine = boot_persistence_engine(&db, nats.nats().await, "se_s098", "pod-s098").await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();

    let key = Uuid::now_v7();
    executor
        .run::<BumpFull>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": key, "tenant": tenant, "amount": 4, "author": "alice"
            }))
            .unwrap(),
        )
        .await
        .expect("the first full-EDA bump creates the counter");

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(FullCounterProjector::NAME, false)],
        ))
        .await
        .expect("the counter session attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");
    let opening: CounterDto = reset_views(&reset)[0]
        .view
        .decode()
        .expect("the opening view decodes");
    assert_eq!(opening.total, 4, "the Reset renders the committed snapshot");

    executor
        .run::<BumpFull>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": key, "tenant": tenant, "amount": 3, "author": "bob"
            }))
            .unwrap(),
        )
        .await
        .expect("the second full-EDA bump commits");

    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("the commit's impact reaches the render side");
    let rendered: CounterDto = upserted(&delta)
        .view
        .decode()
        .expect("the rendered view decodes");
    assert_eq!(
        rendered.total, 7,
        "the render-side Projector::load reads the committed snapshot the write side wrote",
    );

    let mut conn = pool.acquire().await.expect("a connection");
    let written = FullCounterStore::load(&mut conn, &key)
        .await
        .expect("the write-side load succeeds")
        .expect("the counter exists");
    assert_eq!(
        written.0.total, rendered.total,
        "the write-side store and the render-side projector read one committed truth",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
