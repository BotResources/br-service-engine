use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::{
    NoteKey, StagingTransport, SyntheticSource, note_body_runtime,
};
use service_engine::accumulator::ChunkSeq;
use service_engine::error::EngineError;
use sqlx::Row;
use uuid::Uuid;

const RETENTION: Duration = Duration::from_secs(3600);

#[tokio::test]
async fn s14_a_chunk_seq_a_bigint_cannot_store_is_refused_before_it_can_ever_be_durable() {
    let db = TestDb::fresh().await;
    let key = NoteKey {
        assignment_id: Uuid::now_v7(),
        seq: 1,
    };
    let engine = note_body_runtime(
        db.pool_as(db.app_role())
            .await
            .expect("a pool for the engine"),
        StagingTransport::silent(),
        RETENTION,
    );
    let source = SyntheticSource::new(&engine, key.clone());

    let refused = source
        .emit(ChunkSeq::MAX + 1, "over the storable range")
        .expect_err("a sequence a bigint cannot store faithfully never yields a Durable");
    assert!(
        matches!(refused, EngineError::ChunkSeqOutOfRange { .. }),
        "the boundary is refused typed at construction, not silently wrapped negative: {refused:?}"
    );

    let boundary = source
        .emit(ChunkSeq::MAX, "the largest storable sequence")
        .expect("the boundary value is accepted");
    engine.flush_once().await.expect("the engine flushes");
    boundary.await.expect("the boundary chunk is durable");

    let stored: i64 = sqlx::query("SELECT seq FROM service_engine.accumulator_chunk")
        .fetch_one(db.owner_pool())
        .await
        .expect("the boundary chunk row is present")
        .get("seq");
    assert_eq!(
        stored,
        i64::MAX,
        "the sequence stores exactly, never wrapping to a negative the reader would read as a gap"
    );

    db.cleanup().await;
}
