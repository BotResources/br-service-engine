#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::erase::boot_erase_engine;
use engine_twin::await_ready;
use sqlx::{PgPool, Row};
use uuid::Uuid;

const SERVICE: &str = "s127repair";

async fn seed_chunk(pool: &PgPool, note: Uuid) {
    sqlx::query(
        "INSERT INTO service_engine.accumulator_chunk (accumulator, key, seq, chunk) \
         VALUES ('erase_note_stream', $1, 0, $2)",
    )
    .bind(serde_json::Value::String(note.to_string()))
    .bind(serde_json::json!("chunk-0"))
    .execute(pool)
    .await
    .expect("seed a stream chunk the crashed erasure never purged");
}

async fn chunk_count(pool: &PgPool, note: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM service_engine.accumulator_chunk \
         WHERE accumulator = 'erase_note_stream' AND key = $1",
    )
    .bind(serde_json::Value::String(note.to_string()))
    .fetch_one(pool)
    .await
    .expect("count the stream chunks")
}

async fn seal_high_water(pool: &PgPool, note: Uuid) -> Option<i64> {
    sqlx::query_scalar(
        "SELECT high_water FROM service_engine.accumulator_seal \
         WHERE accumulator = 'erase_note_stream' AND key = $1",
    )
    .bind(serde_json::Value::String(note.to_string()))
    .fetch_optional(pool)
    .await
    .expect("read the seal marker")
}

async fn purged_at_set(pool: &PgPool, person: Uuid) -> bool {
    sqlx::query("SELECT purged_at FROM service_engine.person_erasure WHERE person_id = $1")
        .bind(person)
        .fetch_one(pool)
        .await
        .expect("read the erasure record")
        .get::<Option<chrono::DateTime<chrono::Utc>>, _>("purged_at")
        .is_some()
}

#[tokio::test]
async fn s127_the_beat_completes_a_purge_the_erase_committed_but_never_finished() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let person = Uuid::now_v7();
    let note = Uuid::now_v7();
    seed_chunk(&pool, note).await;

    let manifest = serde_json::json!({
        "streams": [{ "accumulator": "erase_note_stream", "key": note.to_string() }],
        "presence": [],
        "blobs": [],
    });
    sqlx::query(
        "INSERT INTO service_engine.person_erasure (person_id, erased_at, manifest, purged_at) \
         VALUES ($1, now(), $2::jsonb, NULL)",
    )
    .bind(person)
    .bind(&manifest)
    .execute(&pool)
    .await
    .expect("record an erasure whose post-commit purge crashed before it ran");

    assert_eq!(chunk_count(&pool, note).await, 1, "the unpurged chunk is present");
    assert!(!purged_at_set(&pool, person).await, "the erasure is not yet purged");

    let engine = boot_erase_engine(&db, nats.nats().await, "se_s127", "pod-s127", SERVICE).await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if chunk_count(&pool, note).await == 0 && purged_at_set(&pool, person).await {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the beat never drained the unpurged erasure",
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        seal_high_water(&pool, note).await,
        Some(1),
        "the repair keeps a seal marker at the stream's high_water so no straggler re-opens the key"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
