mod accumulator_support;

use service_engine::error::AccumulatorError;
use std::time::Duration;

use accumulator_support::{rows, state};
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::stream::token_for;
use conformance_service_engine::sample::{
    NoteBody, NoteKey, StagingTransport, SyntheticSource, note_body_runtime,
};
use service_engine::accumulator::ChunkSeq;
use service_engine::boot::REASON_ACCUMULATOR_NO_SERVICE;
use service_engine::error::EngineError;
use service_engine::{Engine, Readiness, ReadinessHandle};
use uuid::Uuid;

use conformance_service_engine::sample::SamplePrincipal;

const RETENTION: Duration = Duration::from_secs(3600);
const SOON: Duration = Duration::from_secs(5);

fn key() -> NoteKey {
    NoteKey {
        assignment_id: Uuid::now_v7(),
        seq: 1,
    }
}

#[tokio::test]
async fn s146_sealing_an_already_sealed_key_is_refused_with_already_sealed() {
    let db = TestDb::fresh().await;
    let runtime = note_body_runtime(db.app_pool().clone(), StagingTransport::silent(), RETENTION);
    let key = key();
    for chunk in SyntheticSource::new(&runtime, key.clone())
        .emit_range(0..=2)
        .expect("the source buffers 0..=2")
    {
        runtime.flush_once().await.expect("the engine flushes");
        chunk.await.expect("the chunk is durable");
    }

    let mut first = db.app_pool().begin().await.expect("a caller transaction");
    runtime
        .seal_current::<NoteBody>(&mut first, &key)
        .await
        .expect("the first seal joins the caller transaction");
    first.commit().await.expect("the caller commits");

    let mut again = db.app_pool().begin().await.expect("a caller transaction");
    let refused = runtime
        .seal_current::<NoteBody>(&mut again, &key)
        .await
        .expect_err("a second seal must not rewrite the sealed record");
    assert!(
        matches!(
            refused,
            EngineError::Accumulator(AccumulatorError::AlreadySealed { ref accumulator, high_water: 3 })
                if accumulator.as_str() == "note_body"
        ),
        "{refused:?}"
    );
    again.rollback().await.expect("the caller rolls back");
}

#[tokio::test]
async fn s146_a_chunk_beyond_the_declared_last_seq_fails_the_seal_and_is_never_dropped() {
    let db = TestDb::fresh().await;
    let runtime = note_body_runtime(db.app_pool().clone(), StagingTransport::silent(), RETENTION);
    let key = key();
    for chunk in SyntheticSource::new(&runtime, key.clone())
        .emit_range(0..=3)
        .expect("the source buffers 0..=3")
    {
        runtime.flush_once().await.expect("the engine flushes");
        chunk.await.expect("the chunk is durable");
    }
    assert_eq!(rows(db.owner_pool()).await, 4);

    let mut racing = db.app_pool().begin().await.expect("a caller transaction");
    let refused = runtime
        .seal_upto::<NoteBody>(&mut racing, &key, ChunkSeq::new(2).unwrap())
        .await
        .expect_err("a seal that would strand a durable chunk is refused");
    assert!(
        matches!(
            refused,
            EngineError::Accumulator(AccumulatorError::SealChunkBeyondLastSeq {
                ref accumulator,
                last_seq: 2,
                max_seq: 3
            }) if accumulator.as_str() == "note_body"
        ),
        "{refused:?}"
    );
    racing.rollback().await.expect("the caller rolls back");
    assert_eq!(
        rows(db.owner_pool()).await,
        4,
        "the chunk the producer was told is durable is not dropped by the refused seal"
    );

    let mut honest = db.app_pool().begin().await.expect("a caller transaction");
    runtime
        .seal_upto::<NoteBody>(&mut honest, &key, ChunkSeq::new(3).unwrap())
        .await
        .expect("sealing at the true last sequence succeeds");
    honest.commit().await.expect("the caller commits");

    let straggler = SyntheticSource::new(&runtime, key.clone())
        .emit(4, &token_for(4))
        .expect("a straggler buffers");
    runtime.flush_once().await.expect("the engine flushes");
    let refused = straggler
        .await
        .expect_err("a chunk after the seal is refused by the marker");
    assert!(
        matches!(
            refused,
            EngineError::Accumulator(AccumulatorError::SealedChunk {
                seq: 4,
                sealed_high_water: 4
            })
        ),
        "{refused:?}"
    );
}

#[tokio::test]
async fn s146_a_slow_seal_does_not_block_another_keys_flush() {
    let db = TestDb::fresh().await;
    let runtime = note_body_runtime(db.app_pool().clone(), StagingTransport::silent(), RETENTION);
    let held = key();
    let other = key();

    for chunk in SyntheticSource::new(&runtime, held.clone())
        .emit_range(0..=1)
        .expect("the source buffers 0..=1")
    {
        runtime.flush_once().await.expect("the engine flushes");
        chunk.await.expect("the chunk is durable");
    }

    let mut sealing = db.app_pool().begin().await.expect("a caller transaction");
    runtime
        .seal_current::<NoteBody>(&mut sealing, &held)
        .await
        .expect("the seal holds the stream's advisory lock inside the open transaction");

    let other_chunk = SyntheticSource::new(&runtime, other.clone())
        .emit(0, "free")
        .expect("the other key's chunk buffers");
    let held_late = SyntheticSource::new(&runtime, held.clone())
        .emit(2, "stuck")
        .expect("a chunk for the sealing key buffers");

    runtime
        .flush_once()
        .await
        .expect("the flush makes progress on the free key while the seal holds its own lock");
    tokio::time::timeout(SOON, other_chunk)
        .await
        .expect("the free key's chunk becomes durable")
        .expect("the free key's chunk is durable, not parked behind the slow seal");
    assert_eq!(
        state(&runtime, &other).await.state.text,
        "free",
        "the free key folded its chunk while the other key was mid-seal"
    );
    assert_eq!(
        runtime.pending(),
        1,
        "the sealing key's chunk is deferred, not lost, while its lock is held"
    );

    sealing.commit().await.expect("the caller commits the seal");
    runtime.flush_once().await.expect("the engine flushes");
    let refused = tokio::time::timeout(SOON, held_late)
        .await
        .expect("the deferred chunk resolves once the lock is free")
        .expect_err("a chunk on the now-sealed key is refused");
    assert!(
        matches!(
            refused,
            EngineError::Accumulator(AccumulatorError::SealedChunk { seq: 2, .. })
        ),
        "{refused:?}"
    );
}

#[tokio::test]
async fn s146_a_registered_accumulator_without_a_service_fails_loud_at_boot() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;

    let mut engine = Engine::<SamplePrincipal>::boot(
        engine_config("se_s146_serviceless", "pod-s146"),
        db.app_pool().clone(),
        fabric,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the engine boots under the low-privilege app role");
    engine
        .register_accumulator(NoteBody)
        .expect("the accumulator enrolls");
    let readiness = engine.readiness();

    let outcome = engine.run().await;
    assert!(
        matches!(
            outcome,
            Err(EngineError::Accumulator(
                AccumulatorError::AccumulatorWithoutService
            ))
        ),
        "a serviceless engine with an accumulator refuses to run rather than folding in process only: {outcome:?}"
    );
    assert_eq!(
        readiness.snapshot(),
        Readiness::NotReady {
            reason: REASON_ACCUMULATOR_NO_SERVICE.to_string()
        },
        "the boot failure holds readiness DOWN naming the missing service"
    );
}
