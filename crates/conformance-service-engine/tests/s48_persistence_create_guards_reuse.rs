#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_serialization_engine;
use conformance_service_engine::sample::counter::OpenFull;
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
async fn s48_a_unique_constraint_rejects_a_concurrent_duplicate_create_and_its_events_together() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;

    let engine = boot_serialization_engine(&db, nats.nats().await, "se_s48", "pod-s48").await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let key = Uuid::now_v7();
    let input = || {
        serde_json::from_value::<OpenFull>(serde_json::json!({
            "key": key, "tenant": tenant, "amount": 4, "author": "alice"
        }))
        .unwrap()
    };

    let (first, second) = tokio::join!(
        executor.run::<OpenFull>(principal.clone(), input()),
        executor.run::<OpenFull>(principal.clone(), input()),
    );

    let winners = [&first, &second].iter().filter(|o| o.is_ok()).count();
    let losers = [&first, &second].iter().filter(|o| o.is_err()).count();
    assert_eq!(
        winners, 1,
        "creator-generated ids guard reuse: exactly one concurrent create commits",
    );
    assert_eq!(
        losers, 1,
        "the duplicate create is rejected, not silently merged"
    );

    let refused = [first, second]
        .into_iter()
        .find_map(Result::err)
        .expect("one create was refused");
    assert!(
        refused.reason.is_none(),
        "a unique-constraint rejection is a storage violation, not a gate refusal",
    );

    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM sample_counter_full_snapshot WHERE id = $1",
            key
        )
        .await,
        1,
        "exactly one snapshot survives: the rejected create wrote nothing",
    );
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM sample_counter_full_event WHERE counter = $1",
            key
        )
        .await,
        1,
        "the rejected create's events rolled back with its snapshot row: one write, one event",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
