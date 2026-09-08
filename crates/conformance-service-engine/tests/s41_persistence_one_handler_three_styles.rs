#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_persistence_engine;
use conformance_service_engine::sample::counter::{BumpCrud, BumpFull, BumpSoft};
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

async fn total(pool: &PgPool, table: &str, key: Uuid) -> i64 {
    sqlx::query_scalar(&format!("SELECT total FROM {table} WHERE id = $1"))
        .bind(key)
        .fetch_one(pool)
        .await
        .expect("read the total")
}

#[tokio::test]
async fn s41_one_bump_handler_runs_unchanged_over_crud_soft_and_full_eda() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;

    let engine = boot_persistence_engine(&db, nats.nats().await, "se_s41", "pod-s41").await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let crud = Uuid::now_v7();
    let soft = Uuid::now_v7();
    let full = Uuid::now_v7();

    executor
        .run::<BumpCrud>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": crud, "tenant": tenant, "amount": 5, "author": "alice"
            }))
            .unwrap(),
        )
        .await
        .expect("the same handler bumps a CRUD counter");
    executor
        .run::<BumpSoft>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": soft, "tenant": tenant, "amount": 5, "author": "alice"
            }))
            .unwrap(),
        )
        .await
        .expect("the same handler bumps a soft-EDA counter");
    executor
        .run::<BumpFull>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": full, "tenant": tenant, "amount": 5, "author": "alice"
            }))
            .unwrap(),
        )
        .await
        .expect("the same handler bumps a full-EDA counter");

    executor
        .run::<BumpCrud>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": crud, "tenant": tenant, "amount": 3, "author": "bob"
            }))
            .unwrap(),
        )
        .await
        .expect("the second CRUD bump commits");
    executor
        .run::<BumpSoft>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": soft, "tenant": tenant, "amount": 3, "author": "bob"
            }))
            .unwrap(),
        )
        .await
        .expect("the second soft bump commits");
    executor
        .run::<BumpFull>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": full, "tenant": tenant, "amount": 3, "author": "bob"
            }))
            .unwrap(),
        )
        .await
        .expect("the second full bump commits");

    assert_eq!(total(&pool, "sample_counter_crud", crud).await, 8);
    assert_eq!(total(&pool, "sample_counter_soft", soft).await, 8);
    assert_eq!(
        total(&pool, "sample_counter_full_snapshot", full).await,
        8,
        "the three styles reach the same total from one unchanged handler",
    );

    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM sample_counter_soft_fact WHERE counter = $1",
            soft
        )
        .await,
        2,
        "soft EDA appends one fact per change in the same transaction as the row",
    );
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM sample_counter_full_event WHERE counter = $1",
            full
        )
        .await,
        2,
        "full EDA appends one event per change in the same transaction as the snapshot",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
