mod accumulator_support;

use std::sync::Arc;
use std::time::Duration;

use accumulator_support::{rows, state};
use conformance_service_engine::TestDb;
use conformance_service_engine::sample::stream::text_for;
use conformance_service_engine::sample::{
    NoteBody, NoteKey, StagingTransport, SyntheticSource, note_body_runtime,
};
use service_engine::accumulator::{AccumulatorRuntime, ChunkSeq};
use service_engine::error::EngineError;
use service_engine::time;
use sqlx::{PgPool, Row};
use uuid::Uuid;

const RETENTION: Duration = Duration::from_secs(3600);

#[tokio::test]
async fn s25_seal_marker() {
    let db = TestDb::fresh().await;
    let sealed_key = NoteKey {
        assignment_id: Uuid::now_v7(),
        seq: 1,
    };
    let orphan_key = NoteKey {
        assignment_id: Uuid::now_v7(),
        seq: 2,
    };

    let first = engine(db.pool_as(db.app_role()).await.expect("a pool"));
    let observer = engine(db.pool_as(db.app_role()).await.expect("a pool"));
    let source = SyntheticSource::new(&first, sealed_key.clone());
    let chunks = source.emit_range(0..=2).expect("the source buffers 0..=2");
    first.flush_once().await.expect("the engine flushes");
    for chunk in chunks {
        chunk.await.expect("the chunk is durable");
    }
    assert_eq!(state(&first, &sealed_key).await.state.text, text_for(0..=2));

    assert_eq!(
        state(&observer, &sealed_key).await.state.text,
        text_for(0..=2),
        "a second runtime folds the same rows into its own cache"
    );

    let mut tx = db.app_pool().begin().await.expect("a caller transaction");
    first
        .seal::<NoteBody>(&mut tx, &sealed_key)
        .await
        .expect("seal joins the caller transaction");
    tx.commit().await.expect("the caller commits");
    drop(first);

    let restarted_a = engine(db.pool_as(db.app_role()).await.expect("a pool"));
    let restarted_b = engine(db.pool_as(db.app_role()).await.expect("a pool"));
    for restarted in [&restarted_a, &restarted_b] {
        let marker = restarted
            .sealed::<NoteBody>(&sealed_key)
            .await
            .expect("the marker is readable")
            .expect("the seal marker outlives the engine that wrote it");
        assert_eq!(marker.high_water(), ChunkSeq::new(3).unwrap());

        let late = SyntheticSource::new(restarted, sealed_key.clone())
            .emit(3, "after the seal")
            .expect("a late chunk is buffered");
        restarted.flush_once().await.expect("the engine flushes");
        let refused = late.await.expect_err("a late chunk is refused on any pod");
        assert!(
            matches!(
                refused,
                EngineError::SealedChunk {
                    seq: 3,
                    sealed_high_water: 3
                }
            ),
            "{refused:?}"
        );
    }
    assert_eq!(rows(db.owner_pool()).await, 0);

    let orphan = SyntheticSource::new(&restarted_a, orphan_key.clone())
        .emit(0, "never sealed")
        .expect("an orphan chunk is buffered");
    restarted_a.flush_once().await.expect("the engine flushes");
    orphan.await.expect("the orphan chunk is durable");
    assert_eq!(rows(db.owner_pool()).await, 1);

    let swept = restarted_b
        .sweep_expired(service_engine::time::now() + chrono::TimeDelta::hours(2))
        .await
        .expect("the sweep runs");
    assert_eq!(swept.markers, 1, "the marker is gone after chunk_retention");
    assert_eq!(
        swept.chunks, 1,
        "an orphan chunk is swept on the same bound"
    );
    assert_eq!(rows(db.owner_pool()).await, 0);
    for restarted in [&restarted_a, &restarted_b] {
        assert!(
            restarted
                .sealed::<NoteBody>(&sealed_key)
                .await
                .expect("the marker is readable")
                .is_none()
        );
    }

    assert_eq!(
        state(&observer, &sealed_key).await.state.text,
        "",
        "a fold cached before the seal is not served once the rows are gone"
    );

    let reopened = SyntheticSource::new(&restarted_b, sealed_key.clone())
        .emit(0, "a new stream")
        .expect("a chunk is buffered");
    restarted_b.flush_once().await.expect("the engine flushes");
    reopened
        .await
        .expect("past the retention bound the key accepts chunks again");
    for engine in [&restarted_b, &observer] {
        assert_eq!(state(engine, &sealed_key).await.state.text, "a new stream");
    }

    let live_key = NoteKey {
        assignment_id: Uuid::now_v7(),
        seq: 4,
    };
    let live = SyntheticSource::new(&restarted_a, live_key.clone());
    for chunk in live.emit_range(0..=2).expect("the source buffers 0..=2") {
        restarted_a.flush_once().await.expect("the engine flushes");
        chunk.await.expect("the chunk is durable");
    }
    age_chunks(db.owner_pool(), &live_key, 2).await;
    for chunk in live.emit_range(3..=4).expect("the source buffers 3..=4") {
        restarted_a.flush_once().await.expect("the engine flushes");
        chunk.await.expect("the chunk is durable");
    }
    let untouched = restarted_a
        .sweep_expired(time::now())
        .await
        .expect("the sweep runs");
    assert_eq!(
        untouched.chunks, 0,
        "a stream still being written keeps the chunks it staged before the retention bound"
    );
    assert_eq!(rows_for(db.owner_pool(), &live_key).await, 5);
    assert_eq!(
        state(&restarted_a, &live_key).await.state.text,
        text_for(0..=4),
        "the fold of a live stream is not truncated by the retention sweep"
    );

    let raced_key = NoteKey {
        assignment_id: Uuid::now_v7(),
        seq: 5,
    };
    let (gated, gate) = StagingTransport::gated();
    let racer = note_body_runtime(
        db.pool_as(db.app_role()).await.expect("a pool"),
        gated,
        RETENTION,
    );
    let raced = SyntheticSource::new(&racer, raced_key.clone())
        .emit_range(0..=2)
        .expect("the source buffers 0..=2");
    let flushing = tokio::spawn({
        let racer = racer.clone();
        async move { racer.flush_once().await }
    });
    gate.wait_until_staging().await;

    let sealing = tokio::spawn({
        let racer = racer.clone();
        let pool = db.app_pool().clone();
        let key = raced_key.clone();
        async move {
            let mut tx = pool.begin().await.expect("a caller transaction");
            racer
                .seal::<NoteBody>(&mut tx, &key)
                .await
                .expect("seal joins the caller transaction");
            tx.commit().await.expect("the caller commits");
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !sealing.is_finished(),
        "a seal must not slip between a concurrent flush's seal lookup and its insert"
    );

    gate.release();
    flushing
        .await
        .expect("the flush task joins")
        .expect("the engine flushes");
    for chunk in raced {
        chunk.await.expect("the chunk is durable");
    }
    sealing.await.expect("the seal task joins");
    assert_eq!(
        rows_for(db.owner_pool(), &raced_key).await,
        0,
        "the seal that ran after the flush deleted the chunks it was serialised behind"
    );
    assert_eq!(
        racer
            .sealed::<NoteBody>(&raced_key)
            .await
            .expect("the marker is readable")
            .expect("the race ends with a marker")
            .high_water(),
        ChunkSeq::new(3).unwrap(),
        "the marker covers the chunks the concurrent flush committed"
    );

    db.cleanup().await;
}

pub async fn rows_for(pool: &PgPool, key: &NoteKey) -> i64 {
    sqlx::query("SELECT count(*) AS n FROM service_engine.accumulator_chunk WHERE key = $1")
        .bind(serde_json::to_value(key).expect("a note key is json"))
        .fetch_one(pool)
        .await
        .expect("the chunk table is readable")
        .get("n")
}

pub async fn age_chunks(pool: &PgPool, key: &NoteKey, up_to_seq: i64) {
    sqlx::query(
        "UPDATE service_engine.accumulator_chunk SET staged_at = now() - interval '25 hours' \
         WHERE key = $1 AND seq <= $2",
    )
    .bind(serde_json::to_value(key).expect("a note key is json"))
    .bind(up_to_seq)
    .execute(pool)
    .await
    .expect("the chunk rows are aged");
}

fn engine(pool: PgPool) -> Arc<AccumulatorRuntime> {
    note_body_runtime(pool, StagingTransport::silent(), RETENTION)
}
