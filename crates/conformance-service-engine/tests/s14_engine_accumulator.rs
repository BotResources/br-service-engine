#[allow(dead_code)]
mod engine_twin;

use br_util_axum_readiness::ReadinessHandle;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::SamplePrincipal;
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::stream::{text_for, token_for};
use conformance_service_engine::sample::{NoteBody, NoteKey};
use engine_twin::{SOON, await_ready};
use service_engine::Engine;
use service_engine::accumulator::{ChunkSeq, Durable};
use service_engine::error::EngineError;
use uuid::Uuid;

const CHANNEL: &str = "se_s14_engine";

async fn flushed(durable: Durable) -> Result<(), EngineError> {
    tokio::time::timeout(SOON, durable)
        .await
        .expect("the background flush loop commits the chunk within its window")
}

#[tokio::test]
async fn s14_engine_the_run_flush_loop_makes_chunks_durable_folds_them_and_refuses_after_seal() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let mut engine = Engine::<SamplePrincipal>::boot(
        engine_config(CHANNEL, "pod-s14"),
        pool.clone(),
        fabric,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the engine boots under the low-privilege app role");
    engine
        .register_accumulator(NoteBody)
        .expect("the note-body accumulator enrolls");
    let accumulators = engine.accumulators().clone();
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();

    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let key = NoteKey {
        assignment_id: Uuid::now_v7(),
        seq: 1,
    };

    let mut durables = Vec::new();
    for n in 0..=3 {
        durables.push(
            accumulators
                .push_chunk::<NoteBody>(&key, ChunkSeq::new(n).unwrap(), token_for(n))
                .expect("the chunk buffers"),
        );
    }
    for durable in durables {
        flushed(durable)
            .await
            .expect("each chunk resolves only after the run-driven flush commits it");
    }

    let folded = accumulators
        .reader()
        .state::<NoteBody>(&key)
        .await
        .expect("the folded state is readable");
    assert_eq!(folded.state.text, text_for(0..=3));
    assert_eq!(folded.state.folded, (0..=3).collect::<Vec<u64>>());

    let replay = accumulators
        .push_chunk::<NoteBody>(&key, ChunkSeq::new(3).unwrap(), token_for(3))
        .expect("a replay buffers");
    flushed(replay)
        .await
        .expect("a chunk resubmitted with identical content is an idempotent Durable");
    assert_eq!(
        accumulators
            .reader()
            .state::<NoteBody>(&key)
            .await
            .expect("state is readable")
            .state
            .text,
        text_for(0..=3),
        "an identical replay is a no-op for the fold"
    );

    let mut committed = pool.begin().await.expect("a caller transaction");
    accumulators
        .seal::<NoteBody>(&mut committed, &key)
        .await
        .expect("seal joins the caller transaction");
    committed.commit().await.expect("the caller commits");

    let late = accumulators
        .push_chunk::<NoteBody>(&key, ChunkSeq::new(4).unwrap(), token_for(4))
        .expect("a late chunk buffers");
    let refused = flushed(late)
        .await
        .expect_err("a chunk after seal is refused by the flush loop");
    assert!(
        matches!(refused, EngineError::SealedChunk { seq: 4, .. }),
        "a sealed key refuses a later chunk, got {refused:?}"
    );
    assert_eq!(
        accumulators
            .reader()
            .state::<NoteBody>(&key)
            .await
            .expect("state is readable")
            .state
            .text,
        "",
        "a sealed key folds nothing"
    );

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
