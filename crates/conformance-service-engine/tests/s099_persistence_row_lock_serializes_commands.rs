#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_serialization_engine;
use conformance_service_engine::sample::counter::{
    BumpCrud, BumpFull, BumpSoft, OpenCrud, OpenFull, OpenSoft,
};
use conformance_service_engine::sample::render::member;
use engine_twin::await_ready;
use futures_util::future::join_all;
use service_engine::pipeline::MutationExecutor;
use service_engine::principal::Principal;
use sqlx::PgPool;
use uuid::Uuid;

const CONCURRENT: usize = 12;

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

async fn bump_all<M, P>(executor: &MutationExecutor<P>, principal: &P, key: Uuid, tenant: Uuid)
where
    M: service_engine::pipeline::MutationInput<Output = ()> + serde::de::DeserializeOwned,
    P: Principal,
{
    let futures = (0..CONCURRENT).map(|_| {
        executor.run::<M>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": key, "tenant": tenant, "amount": 1, "author": "racer"
            }))
            .expect("the bump input decodes"),
        )
    });
    for outcome in join_all(futures).await {
        outcome.expect("every concurrent command on one key commits, none is lost");
    }
}

#[tokio::test]
async fn s099_concurrent_commands_on_one_key_serialize_in_every_style_without_a_lost_update() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;

    let engine = boot_serialization_engine(&db, nats.nats().await, "se_s099", "pod-s099").await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let crud = Uuid::now_v7();
    let soft = Uuid::now_v7();
    let full = Uuid::now_v7();

    executor
        .run::<OpenCrud>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": crud, "tenant": tenant, "amount": 1, "author": "seed"
            }))
            .unwrap(),
        )
        .await
        .expect("the CRUD create path opens the counter through cx.create");
    executor
        .run::<OpenSoft>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": soft, "tenant": tenant, "amount": 1, "author": "seed"
            }))
            .unwrap(),
        )
        .await
        .expect("the soft-EDA create path writes the row and its first fact");
    executor
        .run::<OpenFull>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": full, "tenant": tenant, "amount": 1, "author": "seed"
            }))
            .unwrap(),
        )
        .await
        .expect("the full-EDA create path appends the first event and snapshots");

    bump_all::<BumpCrud, _>(&executor, &principal, crud, tenant).await;
    bump_all::<BumpSoft, _>(&executor, &principal, soft, tenant).await;
    bump_all::<BumpFull, _>(&executor, &principal, full, tenant).await;

    let expected = 1 + CONCURRENT as i64;

    assert_eq!(
        total(&pool, "sample_counter_crud", crud).await,
        expected,
        "CRUD: the row lock at load serializes the read-modify-write, so no increment is lost",
    );
    assert_eq!(
        total(&pool, "sample_counter_soft", soft).await,
        expected,
        "soft EDA: the row lock serializes concurrent commands on one key",
    );
    assert_eq!(
        total(&pool, "sample_counter_full_snapshot", full).await,
        expected,
        "full EDA: the snapshot row lock serializes concurrent commands on one key",
    );

    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM sample_counter_soft_fact WHERE counter = $1",
            soft
        )
        .await,
        expected,
        "soft EDA: each serialized command appended exactly one fact at a unique sequence",
    );
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM sample_counter_full_event WHERE counter = $1",
            full
        )
        .await,
        expected,
        "full EDA: each serialized command appended exactly one event at a unique sequence",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
