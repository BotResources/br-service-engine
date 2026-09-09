#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_persistence_engine;
use conformance_service_engine::sample::counter::{BumpFull, BumpSoft};
use conformance_service_engine::sample::render::member;
use engine_twin::await_ready;
use sqlx::PgPool;
use uuid::Uuid;

async fn count(pool: &PgPool, sql: &str, key: Uuid) -> i64 {
    sqlx::query_scalar(sql)
        .bind(key)
        .fetch_one(pool)
        .await
        .expect("count query")
}

#[tokio::test]
async fn s094_a_failing_constraint_rolls_back_the_state_row_and_its_events_together() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;

    let engine = boot_persistence_engine(&db, nats.nats().await, "se_s094", "pod-s094").await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let soft = Uuid::now_v7();
    let refused = executor
        .run::<BumpSoft>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": soft, "tenant": tenant, "amount": 150, "author": "alice"
            }))
            .unwrap(),
        )
        .await
        .expect_err("the fact-log ceiling rejects the write");
    assert!(
        refused.reason.is_none(),
        "a storage violation is not a gate refusal"
    );

    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM sample_counter_soft WHERE id = $1",
            soft
        )
        .await,
        0,
        "soft EDA: the row the transaction already wrote is rolled back with the rejected fact",
    );
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM sample_counter_soft_fact WHERE counter = $1",
            soft
        )
        .await,
        0,
        "soft EDA: no fact survives a rolled-back transaction",
    );

    let full = Uuid::now_v7();
    executor
        .run::<BumpFull>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": full, "tenant": tenant, "amount": 150, "author": "alice"
            }))
            .unwrap(),
        )
        .await
        .expect_err("the snapshot state-row ceiling rejects the write");

    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM sample_counter_full_snapshot WHERE id = $1",
            full
        )
        .await,
        0,
        "full EDA: the state table constraint rejects the write",
    );
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM sample_counter_full_event WHERE counter = $1",
            full
        )
        .await,
        0,
        "full EDA: the events the transaction already appended are rejected with the state row",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
