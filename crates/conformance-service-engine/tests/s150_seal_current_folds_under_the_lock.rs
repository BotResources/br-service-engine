use std::time::Duration;

use conformance_service_engine::infra::TestDb;
use conformance_service_engine::sample::{NoteBody, NoteKey, StagingTransport, note_body_runtime};
use service_engine::accumulator::ChunkSeq;
use sqlx::Row;
use uuid::Uuid;

const RETENTION: Duration = Duration::from_secs(3600);

async fn chunk_rows(pool: &sqlx::PgPool, key: &NoteKey) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM service_engine.accumulator_chunk \
         WHERE accumulator = 'note_body' AND key = $1",
    )
    .bind(serde_json::to_value(key).unwrap())
    .fetch_one(pool)
    .await
    .expect("count the chunk rows")
}

#[tokio::test]
async fn s150_seal_current_folds_a_chunk_that_lands_while_it_waits_for_the_lock() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let runtime = note_body_runtime(pool.clone(), StagingTransport::silent(), RETENTION);

    let key = NoteKey {
        assignment_id: Uuid::now_v7(),
        seq: 1,
    };
    for seq in 0..=1u64 {
        runtime
            .push_chunk::<NoteBody>(&key, ChunkSeq::new(seq).unwrap(), format!("<{seq}>"))
            .expect("the chunk buffers");
        runtime.flush_once().await.expect("the engine flushes");
    }

    let lock_id = runtime
        .stream_lock_id::<NoteBody>(&key)
        .expect("the per-key stream lock id is derivable");

    let mut blocker = pool.begin().await.expect("open the blocking transaction");
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(lock_id)
        .execute(&mut *blocker)
        .await
        .expect("the test holds the stream's advisory lock, so the seal must wait for it");

    let seal = tokio::spawn({
        let runtime = runtime.clone();
        let pool = pool.clone();
        let key = key.clone();
        async move {
            let mut tx = pool.begin().await.expect("open the seal transaction");
            let (_sealed, state) = runtime
                .seal_current::<NoteBody>(&mut tx, &key)
                .await
                .expect("the seal folds and writes its marker under the lock");
            tx.commit().await.expect("the seal commits");
            state
        }
    });

    tokio::time::sleep(Duration::from_millis(250)).await;

    sqlx::query(
        "INSERT INTO service_engine.accumulator_chunk (accumulator, key, seq, chunk, staged_at) \
         VALUES ('note_body', $1, 2, $2, now())",
    )
    .bind(serde_json::to_value(&key).unwrap())
    .bind(serde_json::Value::String("<2>".to_string()))
    .execute(&pool)
    .await
    .expect("a chunk becomes durable while the seal waits for the lock");

    blocker
        .commit()
        .await
        .expect("releasing the lock lets the seal proceed");

    let state = seal.await.expect("join the seal task");
    assert_eq!(
        state.text, "<0><1><2>",
        "the chunk that landed during the lock-wait window is folded into the sealed state, \
         not read before the lock and lost"
    );
    assert_eq!(
        state.folded,
        vec![0, 1, 2],
        "every sealed chunk is present in the returned state, in order"
    );
    assert_eq!(
        chunk_rows(&pool, &key).await,
        0,
        "the seal deleted the chunks it folded, including the one that raced in"
    );
    let high_water: i64 = sqlx::query(
        "SELECT high_water FROM service_engine.accumulator_seal \
         WHERE accumulator = 'note_body' AND key = $1",
    )
    .bind(serde_json::to_value(&key).unwrap())
    .fetch_one(&pool)
    .await
    .expect("the seal marker is written")
    .get("high_water");
    assert_eq!(
        high_water, 3,
        "the marker's high water counts the raced-in chunk the returned state also folded"
    );

    db.cleanup().await;
}
