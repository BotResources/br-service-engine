#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_persistence_engine;
use conformance_service_engine::sample::counter::{BumpFull, FullCounterProjector};
use conformance_service_engine::sample::render::{
    attach_request, member, next_delta, reset_views, window,
};
use engine_twin::{SOON, await_ready};
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct CounterDto {
    total: i64,
}

async fn total(pool: &PgPool, key: Uuid) -> i64 {
    sqlx::query_scalar("SELECT total FROM sample_counter_full_snapshot WHERE id = $1")
        .bind(key)
        .fetch_one(pool)
        .await
        .expect("read the snapshot total")
}

#[tokio::test]
async fn s130_a_render_frame_takes_no_row_lock_while_a_mutation_does() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;

    let engine = boot_persistence_engine(&db, nats.nats().await, "se_s130", "pod-s130").await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();

    let key = Uuid::now_v7();
    executor
        .run::<BumpFull>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": key, "tenant": tenant, "amount": 4, "author": "seed"
            }))
            .unwrap(),
        )
        .await
        .expect("the first full-EDA bump creates the counter");

    let mut holder = pool.begin().await.expect("open the holder transaction");
    sqlx::query("SELECT id FROM sample_counter_full_snapshot WHERE id = $1 FOR UPDATE")
        .bind(key)
        .execute(&mut *holder)
        .await
        .expect("take the snapshot row lock out of band");

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
        .expect("the opening Reset renders while the row lock is held");
    let opening: CounterDto = reset_views(&reset)[0]
        .view
        .decode()
        .expect("the opening view decodes");
    assert_eq!(
        opening.total, 4,
        "the render frame read the committed snapshot through read_many without waiting on the \
         write lock, so a render takes no row lock",
    );

    let contended = executor
        .run::<BumpFull>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": key, "tenant": tenant, "amount": 1, "author": "racer"
            }))
            .unwrap(),
        )
        .await;
    assert!(
        contended.is_err(),
        "the write side takes the row lock: a mutation on the held key times out (lock_timeout) \
         instead of committing",
    );
    assert_eq!(
        total(&pool, key).await,
        4,
        "the contended mutation did not commit while the lock was held",
    );

    holder.rollback().await.expect("release the row lock");

    executor
        .run::<BumpFull>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": key, "tenant": tenant, "amount": 1, "author": "racer"
            }))
            .unwrap(),
        )
        .await
        .expect("the mutation commits once the lock is released");
    assert_eq!(
        total(&pool, key).await,
        5,
        "the retried mutation committed after the row lock released",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
