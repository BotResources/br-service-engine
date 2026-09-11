#[allow(dead_code)]
mod engine_twin;

use std::sync::Arc;
use std::time::{Duration, Instant};

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::SamplePrincipal;
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::note::NoteKey;
use conformance_service_engine::sample::stream::NoteBody;
use engine_twin::await_ready;
use service_engine::accumulator::AccumulatorRuntime;
use service_engine::nats::{Nats, StreamFrame};
use service_engine::{Engine, ReadinessHandle};
use uuid::Uuid;

const CHANNEL: &str = "se_s140_ingress";
const SERVICE: &str = "s140ingress";

type Pod = (
    Arc<AccumulatorRuntime>,
    ReadinessHandle,
    Arc<tokio::sync::Notify>,
    tokio::task::JoinHandle<Result<(), service_engine::error::EngineError>>,
);

async fn boot_pod(db: &TestDb, nats: Nats, pod: &str) -> Pod {
    let mut engine = Engine::<SamplePrincipal>::boot(
        engine_config(CHANNEL, pod).with_service(SERVICE),
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the ingress pod boots under the low-privilege app role");
    engine
        .register_accumulator(NoteBody)
        .expect("the note-body accumulator enrolls");
    let accumulators = engine.accumulators().clone();
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;
    (accumulators, readiness, shutdown, running)
}

async fn publish_chunk(producer: &Nats, key: &NoteKey, seq: u64, token: &str) {
    let frame = StreamFrame {
        accumulator: NoteBody::NAME.as_str().to_string(),
        key: serde_json::to_value(key).expect("a note key serializes"),
        seq,
        chunk: serde_json::json!(token),
    };
    producer
        .publish_chunk(SERVICE, &frame)
        .await
        .expect("a separate producer publishes the chunk onto the lane-A stream");
}

async fn folded_text(accumulators: &AccumulatorRuntime, key: &NoteKey) -> String {
    accumulators
        .reader()
        .state::<NoteBody>(key)
        .await
        .expect("the folded state is readable")
        .state
        .text
}

#[tokio::test]
async fn s140_a_separate_producer_reaches_every_pod_and_a_chunk_after_seal_is_refused() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    nats.provision_streaming(SERVICE, Duration::from_secs(300))
        .await;

    let (acc_a, _ready_a, stop_a, run_a) = boot_pod(&db, nats.nats().await, "pod-s140-a").await;
    let (acc_b, _ready_b, stop_b, run_b) = boot_pod(&db, nats.nats().await, "pod-s140-b").await;

    let producer = nats.nats().await;
    let key = NoteKey {
        assignment_id: Uuid::now_v7(),
        seq: 1,
    };

    for (seq, token) in ["a", "b", "c"].iter().enumerate() {
        publish_chunk(&producer, &key, seq as u64, token).await;
    }

    for (label, acc) in [("pod-a", &acc_a), ("pod-b", &acc_b)] {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if folded_text(acc, &key).await == "abc" {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{label} never folded the chunks a separate producer streamed over NATS"
            );
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
    }

    let mut tx = db
        .app_pool()
        .begin()
        .await
        .expect("open a seal transaction");
    acc_a
        .seal_current::<NoteBody>(&mut tx, &key)
        .await
        .expect("one pod seals the key through the direct lane");
    tx.commit().await.expect("the seal commits");

    publish_chunk(&producer, &key, 3, "d").await;

    tokio::time::sleep(Duration::from_millis(400)).await;
    for (label, acc) in [("pod-a", &acc_a), ("pod-b", &acc_b)] {
        assert_eq!(
            folded_text(acc, &key).await,
            "",
            "{label} drops the chunk that arrived after the seal; a late redelivery cannot resurrect the key"
        );
    }

    stop_a.notify_one();
    stop_b.notify_one();
    run_a.await.expect("pod a joins").expect("pod a run ok");
    run_b.await.expect("pod b joins").expect("pod b run ok");
    drop(nats);
    db.cleanup().await;
}
