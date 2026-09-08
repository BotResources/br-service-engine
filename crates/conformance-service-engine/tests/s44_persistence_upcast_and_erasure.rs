use conformance_service_engine::infra::TestDb;
use conformance_service_engine::sample::counter::FullCounterStore;
use conformance_service_engine::sample::erase_author;
use service_engine::persistence::Persistence;
use sqlx::{PgPool, Row};
use uuid::Uuid;

async fn snapshot(
    pool: &PgPool,
    key: Uuid,
    tenant: Uuid,
    total: i64,
    last_author: Option<&str>,
    version: i64,
) {
    sqlx::query(
        "INSERT INTO sample_counter_full_snapshot (id, tenant, total, closed, last_author, version) \
         VALUES ($1, $2, $3, false, $4, $5)",
    )
    .bind(key)
    .bind(tenant)
    .bind(total)
    .bind(last_author)
    .bind(version)
    .execute(pool)
    .await
    .expect("insert the snapshot");
}

async fn event(pool: &PgPool, key: Uuid, seq: i64, version: i32, payload: serde_json::Value) {
    sqlx::query(
        "INSERT INTO sample_counter_full_event (counter, seq, version, payload) VALUES ($1, $2, $3, $4)",
    )
    .bind(key)
    .bind(seq)
    .bind(version)
    .bind(payload)
    .execute(pool)
    .await
    .expect("insert the event");
}

#[tokio::test]
async fn s44_an_old_event_version_is_upcast_at_read_time() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let tenant = Uuid::now_v7();
    let key = Uuid::now_v7();

    snapshot(&pool, key, tenant, 0, None, 0).await;
    event(
        &pool,
        key,
        1,
        1,
        serde_json::json!({ "kind": "Bumped", "by": 7, "who": "alice" }),
    )
    .await;

    let mut conn = pool.acquire().await.expect("a connection");
    let counter = FullCounterStore::load(&mut conn, &key)
        .await
        .expect("the v1 event upcasts and replays")
        .expect("the counter exists");
    assert_eq!(counter.0.total, 7, "the v1 'by' field upcasts to 'amount'");
    assert_eq!(
        counter.0.last_author.as_deref(),
        Some("alice"),
        "the v1 'who' field upcasts to 'author'",
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s44_erasure_rewrites_the_log_in_place_and_leaves_it_readable() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let tenant = Uuid::now_v7();
    let key = Uuid::now_v7();

    snapshot(&pool, key, tenant, 8, Some("bob"), 2).await;
    event(
        &pool,
        key,
        1,
        2,
        serde_json::json!({ "kind": "Bumped", "amount": 5, "author": "alice" }),
    )
    .await;
    event(
        &pool,
        key,
        2,
        2,
        serde_json::json!({ "kind": "Bumped", "amount": 3, "author": "bob" }),
    )
    .await;

    let mut tx = pool.begin().await.expect("a transaction");
    let affected = erase_author(&mut tx, "alice")
        .await
        .expect("erasure rewrites the person's events in place and re-snapshots");
    tx.commit().await.expect("commit the erasure");
    assert_eq!(affected, vec![key]);

    let leaked: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sample_counter_full_event WHERE counter = $1 AND payload ->> 'author' = 'alice'",
    )
    .bind(key)
    .fetch_one(&pool)
    .await
    .expect("scan for the erased person");
    assert_eq!(leaked, 0, "the person's name is gone from the log");

    let rewritten: String = sqlx::query(
        "SELECT payload ->> 'author' AS author FROM sample_counter_full_event WHERE counter = $1 AND seq = 1",
    )
    .bind(key)
    .fetch_one(&pool)
    .await
    .expect("read the rewritten event")
    .get("author");
    assert_eq!(rewritten, "erased");

    let mut conn = pool.acquire().await.expect("a connection");
    let counter = FullCounterStore::load(&mut conn, &key)
        .await
        .expect("the rewritten log still replays: it is readable")
        .expect("the counter exists");
    assert_eq!(
        counter.0.total, 8,
        "erasure anonymizes fields, it does not lose amounts"
    );

    db.cleanup().await;
}
