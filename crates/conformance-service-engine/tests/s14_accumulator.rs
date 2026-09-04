mod accumulator_support;

use std::sync::Arc;
use std::time::Duration;

use accumulator_support::{rows, state};
use conformance_service_engine::TestDb;
use conformance_service_engine::sample::stream::{text_for, token_for};
use conformance_service_engine::sample::{
    NoteBody, NoteKey, SAMPLE_CHANNEL, StagingTransport, SyntheticSource, note_body_runtime,
};
use futures_util::FutureExt;
use service_engine::accumulator::{ChunkSeq, Durable};
use service_engine::error::EngineError;
use service_engine::impact::Impact;
use sqlx::postgres::PgListener;
use tokio::sync::Notify;
use uuid::Uuid;

const RETENTION: Duration = Duration::from_secs(3600);

#[tokio::test]
async fn s14_accumulator() {
    let db = TestDb::fresh().await;
    let key = NoteKey {
        assignment_id: Uuid::now_v7(),
        seq: 1,
    };

    let engine_a = note_body_runtime(
        db.pool_as(db.app_role())
            .await
            .expect("a pool for engine A"),
        StagingTransport::notifying(SAMPLE_CHANNEL),
        RETENTION,
    );
    let engine_b = note_body_runtime(
        db.pool_as(db.app_role())
            .await
            .expect("a pool for engine B"),
        StagingTransport::silent(),
        RETENTION,
    );
    let mut listener = PgListener::connect(&db.url_as(db.app_role()))
        .await
        .expect("a listener on the fresh database");
    listener
        .listen(SAMPLE_CHANNEL.as_str())
        .await
        .expect("LISTEN on the sample channel");

    let source_a = SyntheticSource::new(&engine_a, key.clone());
    let source_b = SyntheticSource::new(&engine_b, key.clone());

    let mut high = source_a
        .emit_range(5..=10)
        .expect("engine A buffers 5..=10");
    let mut low = source_b.emit_range(1..=4).expect("engine B buffers 1..=4");

    assert!(none_resolved(&mut high) && none_resolved(&mut low));
    assert_eq!(rows(db.owner_pool()).await, 0, "nothing is written yet");
    assert!(
        notification(&mut listener).await.is_none(),
        "no impact is notified before the chunks are durable"
    );

    engine_a.flush_once().await.expect("engine A flushes");
    assert!(all_ok(&mut high), "engine A's chunks are durable");
    assert!(
        none_resolved(&mut low),
        "engine B's chunks wait for engine B's own flush"
    );
    assert_eq!(rows(db.owner_pool()).await, 6);
    let notified = notification(&mut listener)
        .await
        .expect("the flush staged its impact inside the flush transaction");
    assert!(
        notified.iter().any(|impact| matches!(
            impact,
            Impact::ResourceChanged { noun, .. } if noun.as_str() == "note"
        )),
        "the staged impact addresses the accumulator's noun: {notified:?}"
    );

    for engine in [&engine_a, &engine_b] {
        let accumulated = state(engine, &key).await;
        assert_eq!(accumulated.contiguous_to, None);
        assert!(accumulated.gap, "the fold never starts past a hole");
        assert_eq!(accumulated.state.text, "");
    }

    engine_b.flush_once().await.expect("engine B flushes");
    assert!(all_ok(&mut low), "engine B's chunks are durable");
    for engine in [&engine_a, &engine_b] {
        let accumulated = state(engine, &key).await;
        assert!(
            accumulated.gap && accumulated.contiguous_to.is_none(),
            "sequence 0 is still missing, so the fold has not started"
        );
    }

    let head = source_b
        .emit(0, &token_for(0))
        .expect("engine B buffers the missing head");
    engine_b
        .flush_once()
        .await
        .expect("engine B flushes the head");
    head.await.expect("the head is durable");

    for engine in [&engine_a, &engine_b] {
        let accumulated = state(engine, &key).await;
        assert_eq!(accumulated.contiguous_to, Some(ChunkSeq::new(10).unwrap()));
        assert!(!accumulated.gap);
        assert_eq!(accumulated.state.text, text_for(0..=10));
        assert_eq!(accumulated.state.folded, (0..=10).collect::<Vec<u64>>());
    }

    let before = state(&engine_a, &key).await.state;
    let replay = source_b
        .emit(3, &token_for(3))
        .expect("a genuine replay is buffered");
    engine_b
        .flush_once()
        .await
        .expect("engine B flushes the replay");
    replay.await.expect(
        "a chunk resubmitted with identical content is an idempotent Durable, not an error",
    );
    assert_eq!(
        rows(db.owner_pool()).await,
        11,
        "an identical replay inserts nothing"
    );
    assert_eq!(
        state(&engine_a, &key).await.state,
        before,
        "an identical replay is a no-op for the fold"
    );

    let diverged = source_b
        .emit(3, "diverged")
        .expect("a divergent chunk is buffered");
    engine_b
        .flush_once()
        .await
        .expect("engine B flushes the divergent chunk");
    let conflict = diverged
        .await
        .expect_err("a chunk at an already-durable seq with different content is a typed conflict");
    assert!(
        matches!(conflict, EngineError::ChunkConflict { seq: 3, .. }),
        "Durable means this payload is durable, not that some payload occupies the sequence: \
         {conflict:?}"
    );
    assert_eq!(
        rows(db.owner_pool()).await,
        11,
        "a conflicting chunk is refused, never overwriting or adding a row"
    );
    assert_eq!(
        state(&engine_a, &key).await.state,
        before,
        "a refused divergent chunk never enters the fold"
    );

    let mut open = db.app_pool().begin().await.expect("a caller transaction");
    engine_a
        .seal::<NoteBody>(&mut open, &key)
        .await
        .expect("seal joins the caller transaction");
    assert_eq!(
        rows(db.owner_pool()).await,
        11,
        "seal deletes nothing outside the caller transaction"
    );
    open.rollback().await.expect("the caller rolls back");
    assert_eq!(
        rows(db.owner_pool()).await,
        11,
        "a rolled back seal is no seal"
    );
    assert!(
        engine_a
            .sealed::<NoteBody>(&key)
            .await
            .expect("the marker is readable")
            .is_none()
    );

    let mut committed = db.app_pool().begin().await.expect("a caller transaction");
    engine_a
        .seal::<NoteBody>(&mut committed, &key)
        .await
        .expect("seal joins the caller transaction");
    committed.commit().await.expect("the caller commits");
    assert_eq!(
        rows(db.owner_pool()).await,
        0,
        "seal deleted the chunk rows"
    );
    assert_eq!(
        engine_a
            .sealed::<NoteBody>(&key)
            .await
            .expect("the marker is readable")
            .expect("the seal marker is durable")
            .high_water(),
        ChunkSeq::new(11).unwrap()
    );

    let late = source_a
        .emit(11, "too late")
        .expect("a late chunk is buffered");
    engine_a.flush_once().await.expect("engine A flushes");
    let refused = late.await.expect_err("a chunk after seal is refused");
    assert!(
        matches!(
            refused,
            EngineError::SealedChunk {
                seq: 11,
                sealed_high_water: 11
            }
        ),
        "{refused:?}"
    );
    assert_eq!(
        rows(db.owner_pool()).await,
        0,
        "the late chunk was not written"
    );
    assert_eq!(
        state(&engine_a, &key).await.state.text,
        "",
        "a sealed key folds nothing on any pod"
    );

    let streamed = NoteKey {
        assignment_id: Uuid::now_v7(),
        seq: 2,
    };
    let shutdown = Arc::new(Notify::new());
    let worker = tokio::spawn(
        engine_b
            .clone()
            .run(Duration::from_millis(20), shutdown.clone()),
    );
    let batched = SyntheticSource::new(&engine_b, streamed.clone())
        .emit_range(0..=3)
        .expect("the source buffers four chunks");
    for durable in batched {
        tokio::time::timeout(Duration::from_secs(5), durable)
            .await
            .expect("the background worker flushes within its window")
            .expect("the chunk is durable");
    }
    assert_eq!(
        state(&engine_b, &streamed).await.state.text,
        text_for(0..=3)
    );
    shutdown.notify_one();
    tokio::time::timeout(Duration::from_secs(5), worker)
        .await
        .expect("the worker stops on shutdown")
        .expect("the worker task did not panic");

    db.cleanup().await;
}

async fn notification(listener: &mut PgListener) -> Option<Vec<Impact>> {
    let frame = tokio::time::timeout(Duration::from_millis(300), listener.recv())
        .await
        .ok()?
        .expect("the listener stays connected");
    Some(serde_json::from_str(frame.payload()).expect("the payload is a list of impacts"))
}

fn none_resolved(durables: &mut [Durable]) -> bool {
    durables
        .iter_mut()
        .all(|durable| (&mut *durable).now_or_never().is_none())
}

fn all_ok(durables: &mut [Durable]) -> bool {
    durables
        .iter_mut()
        .all(|durable| matches!((&mut *durable).now_or_never(), Some(Ok(()))))
}
