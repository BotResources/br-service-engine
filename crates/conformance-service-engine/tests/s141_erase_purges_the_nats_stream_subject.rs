#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::erase::{EraseNoteStream, boot_erase_lane_a_engine, seed_note};
use engine_twin::await_ready;
use service_engine::PersonId;
use service_engine::accumulator::AccumulatorRuntime;
use service_engine::nats::{Nats, StreamFrame};
use sqlx::{PgPool, Row};
use uuid::Uuid;

const SERVICE: &str = "s141erasepurge";

async fn folded_text(accumulators: &AccumulatorRuntime, note: Uuid) -> String {
    accumulators
        .reader()
        .state::<EraseNoteStream>(&note)
        .await
        .expect("the folded state is readable")
        .state
}

async fn seal_purged(pool: &PgPool, note: Uuid) -> Option<bool> {
    sqlx::query(
        "SELECT purged_at FROM service_engine.accumulator_seal \
         WHERE accumulator = 'erase_note_stream' AND key = $1",
    )
    .bind(serde_json::Value::String(note.to_string()))
    .fetch_optional(pool)
    .await
    .expect("read the seal marker")
    .map(|row| {
        row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("purged_at")
            .is_some()
    })
}

async fn chunk_count(pool: &PgPool, note: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM service_engine.accumulator_chunk \
         WHERE accumulator = 'erase_note_stream' AND key = $1",
    )
    .bind(serde_json::Value::String(note.to_string()))
    .fetch_one(pool)
    .await
    .expect("count the folded chunks")
}

async fn publish_chunk(producer: &Nats, note: Uuid, seq: u64, chunk: &str) {
    let frame = StreamFrame {
        accumulator: "erase_note_stream".to_string(),
        key: serde_json::json!(note),
        seq,
        chunk: serde_json::json!(chunk),
    };
    producer
        .publish_chunk(SERVICE, &frame)
        .await
        .expect("a separate producer streams the person's chunk onto the lane-A stream");
}

#[tokio::test]
async fn s141_erasing_a_person_purges_their_chunks_from_the_lane_a_nats_stream() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    nats.provision_streaming(SERVICE, Duration::from_secs(300))
        .await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let person = Uuid::now_v7();
    let note = Uuid::now_v7();
    seed_note(&pool, note, person, tenant, "a note the person authored").await;

    let engine =
        boot_erase_lane_a_engine(&db, nats.nats().await, "se_s141", "pod-s141", SERVICE).await;
    let accumulators = engine.accumulators().clone();
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let eraser = engine.eraser();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let producer = nats.nats().await;
    publish_chunk(&producer, note, 0, "secret ").await;
    publish_chunk(&producer, note, 1, "words").await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if folded_text(&accumulators, note).await == "secret words" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the pod never folded the chunks a separate producer streamed over NATS",
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    let outcome = eraser
        .erase(PersonId(person))
        .await
        .expect("erase runs against the lane-A erase engine");
    assert!(outcome.fresh, "the first erase records the erasure fact");
    assert_eq!(
        outcome.rows_erased, 1,
        "the person's one note is erased, sealing its accumulator key"
    );
    assert_eq!(
        chunk_count(&pool, note).await,
        0,
        "erase deletes the folded chunk rows in the same transaction"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if seal_purged(&pool, note).await == Some(true) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the beat never purged the erased key's NATS subject; the seal stayed unpurged",
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
    }

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
