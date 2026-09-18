use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{BumpCrud, boot_serialization_engine};
use futures_util::future::join_all;
use sqlx::PgPool;
use uuid::Uuid;

const BUMPS: i64 = 8;

async fn total(pool: &PgPool, key: Uuid) -> i64 {
    sqlx::query_scalar("SELECT total FROM sample_counter_crud WHERE id = $1")
        .bind(key)
        .fetch_one(pool)
        .await
        .expect("read the crud counter total")
}

#[tokio::test]
async fn s201_concurrent_read_modify_write_bumps_serialize_and_lose_nothing() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let engine = boot_serialization_engine(&db, nats.nats().await, "se_s201", "pod-s201").await;
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());

    let key = Uuid::now_v7();
    let bumps = (0..BUMPS).map(|_| {
        executor.run::<BumpCrud>(
            principal.clone(),
            BumpCrud {
                key,
                tenant,
                amount: 1,
                author: "racer".to_string(),
            },
        )
    });
    for outcome in join_all(bumps).await {
        outcome.expect("every bump commits under the row lock the crud store takes on load");
    }

    assert_eq!(
        total(&pool, key).await,
        BUMPS,
        "the store's row_lock serializes the read-modify-write bumps, so none is lost"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
