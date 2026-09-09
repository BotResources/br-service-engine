use conformance_service_engine::infra::TestDb;
use conformance_service_engine::sample::counter::FullCounterStore;
use conformance_service_engine::sample::replay_from_scratch;
use service_engine::error::EngineError;
use service_engine::persistence::Persistence;
use sqlx::PgPool;
use uuid::Uuid;

async fn snapshot(pool: &PgPool, key: Uuid, tenant: Uuid, total: i64, closed: bool, version: i64) {
    sqlx::query(
        "INSERT INTO sample_counter_full_snapshot (id, tenant, total, closed, last_author, version) \
         VALUES ($1, $2, $3, $4, NULL, $5)",
    )
    .bind(key)
    .bind(tenant)
    .bind(total)
    .bind(closed)
    .bind(version)
    .execute(pool)
    .await
    .expect("insert the snapshot");
}

async fn event(pool: &PgPool, key: Uuid, seq: i64, payload: serde_json::Value) {
    sqlx::query(
        "INSERT INTO sample_counter_full_event (counter, seq, version, payload) VALUES ($1, $2, 2, $3)",
    )
    .bind(key)
    .bind(seq)
    .bind(payload)
    .execute(pool)
    .await
    .expect("insert the event");
}

fn bumped(amount: i64, author: &str) -> serde_json::Value {
    serde_json::json!({ "kind": "Bumped", "amount": amount, "author": author })
}

#[tokio::test]
async fn s095_replay_from_a_lagging_snapshot_equals_replay_from_scratch() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let tenant = Uuid::now_v7();
    let key = Uuid::now_v7();

    snapshot(&pool, key, tenant, 5, false, 1).await;
    event(&pool, key, 1, bumped(5, "alice")).await;
    event(&pool, key, 2, bumped(3, "bob")).await;
    event(&pool, key, 3, bumped(2, "carol")).await;

    let mut conn = pool.acquire().await.expect("a connection");
    let from_snapshot = FullCounterStore::load(&mut conn, &key)
        .await
        .expect("load replays the events above the lagging snapshot")
        .expect("the counter exists");
    let from_scratch = replay_from_scratch(&mut conn, &key, tenant)
        .await
        .expect("replay from the first event");

    assert_eq!(from_snapshot.0.total, 10);
    assert_eq!(from_snapshot.0.version, 3);
    assert_eq!(
        from_snapshot.0.total, from_scratch.total,
        "replaying from the snapshot yields the same state as replaying the whole log",
    );
    assert_eq!(from_snapshot.0.version, from_scratch.version);

    db.cleanup().await;
}

#[tokio::test]
async fn s095_the_hydration_barrier_refuses_a_malformed_log() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let tenant = Uuid::now_v7();
    let key = Uuid::now_v7();

    snapshot(&pool, key, tenant, 0, false, 0).await;
    event(&pool, key, 1, serde_json::json!({ "kind": "Closed" })).await;
    event(&pool, key, 2, bumped(5, "alice")).await;

    let mut conn = pool.acquire().await.expect("a connection");
    let refused = FullCounterStore::load(&mut conn, &key)
        .await
        .expect_err("a Bumped applied after Closed fails the second barrier at hydration");
    let EngineError::Service(inner) = &refused else {
        panic!("the hydration barrier surfaces as a service error, got {refused:?}");
    };
    assert!(
        inner.to_string().contains("hydration"),
        "the malformed log is refused by the hydration check, got {inner}",
    );

    db.cleanup().await;
}
