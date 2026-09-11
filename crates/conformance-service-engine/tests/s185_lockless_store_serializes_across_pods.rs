#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_serialization_engine;
use conformance_service_engine::sample::counter::{BumpLockless, OpenLockless};
use conformance_service_engine::sample::render::member;
use engine_twin::await_ready;
use futures_util::future::join_all;
use service_engine::pipeline::MutationExecutor;
use service_engine::principal::Principal;
use sqlx::PgPool;
use uuid::Uuid;

const PER_ENGINE: usize = 8;

async fn total(pool: &PgPool, key: Uuid) -> i64 {
    sqlx::query_scalar("SELECT total FROM sample_counter_crud WHERE id = $1")
        .bind(key)
        .fetch_one(pool)
        .await
        .expect("read the counter total")
}

async fn bump_many<P: Principal>(
    executor: &MutationExecutor<P>,
    principal: &P,
    key: Uuid,
    tenant: Uuid,
) {
    let futures = (0..PER_ENGINE).map(|_| {
        executor.run::<BumpLockless>(
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
async fn s185_a_lock_less_store_serializes_concurrent_commands_on_one_key_across_two_pods() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;

    let engine_a =
        boot_serialization_engine(&db, nats.nats().await, "se_s185_a", "pod-s185a").await;
    let engine_b =
        boot_serialization_engine(&db, nats.nats().await, "se_s185_b", "pod-s185b").await;

    let readiness_a = engine_a.readiness();
    let readiness_b = engine_b.readiness();
    let shutdown_a = engine_a.shutdown_handle();
    let shutdown_b = engine_b.shutdown_handle();
    let executor_a = engine_a.mutation_executor();
    let executor_b = engine_b.mutation_executor();
    let running_a = tokio::spawn(engine_a.run());
    let running_b = tokio::spawn(engine_b.run());
    await_ready(&readiness_a).await;
    await_ready(&readiness_b).await;

    let key = Uuid::now_v7();
    executor_a
        .run::<OpenLockless>(
            principal.clone(),
            serde_json::from_value(serde_json::json!({
                "key": key, "tenant": tenant, "amount": 1, "author": "seed"
            }))
            .unwrap(),
        )
        .await
        .expect("the lock-less store opens the counter through cx.create");

    tokio::join!(
        bump_many(&executor_a, &principal, key, tenant),
        bump_many(&executor_b, &principal, key, tenant),
    );

    let expected = 1 + 2 * PER_ENGINE as i64;
    assert_eq!(
        total(&pool, key).await,
        expected,
        "the store overrides no `lock` and its `load` takes no `FOR UPDATE`, so only the engine's \
         per-key transaction advisory lock serialises the read-modify-write; every increment from \
         both pods lands and none is lost",
    );

    shutdown_a.notify_one();
    shutdown_b.notify_one();
    running_a.await.expect("join a").expect("run a returns Ok");
    running_b.await.expect("join b").expect("run b returns Ok");
    drop(nats);
    db.cleanup().await;
}
