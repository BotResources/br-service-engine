#[allow(dead_code)]
mod engine_twin;

use std::time::{Duration, Instant};

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::SamplePrincipal;
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::note::NoteKey;
use conformance_service_engine::sample::stream::NoteBody;
use engine_twin::await_ready;
use service_engine::accumulator::AccumulatorRuntime;
use service_engine::nats::{Nats, StreamFrame};
use service_engine::{Engine, Readiness, ReadinessHandle};
use uuid::Uuid;

const CHANNEL: &str = "se_s157_supervision";
const SERVICE: &str = "s148supervision";

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
        .expect("a producer publishes onto the lane-A stream");
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

async fn await_folded(accumulators: &AccumulatorRuntime, key: &NoteKey, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if folded_text(accumulators, key).await == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the lane-A ingress never folded up to {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

async fn await_not_ready(readiness: &ReadinessHandle) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if readiness.snapshot() != Readiness::Ready {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "readiness never fell after the broker was killed past its grace window"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn s157_a_supervised_lane_a_consumer_recovers_after_the_broker_is_killed_and_restarted() {
    let db = TestDb::fresh().await;
    let mut nats = TestNats::spawn().await;
    nats.provision().await;
    nats.provision_streaming(SERVICE, Duration::from_secs(300))
        .await;

    let config = engine_config(CHANNEL, "pod-s148")
        .with_service(SERVICE)
        .with_nats_grace(Duration::from_secs(1))
        .with_beat(Duration::from_millis(200));
    let mut engine = Engine::<SamplePrincipal>::boot(
        config,
        db.app_pool().clone(),
        nats.nats().await,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the supervised pod boots under the low-privilege app role");
    engine
        .register_accumulator(NoteBody)
        .expect("the note-body accumulator enrolls");
    let accumulators = engine.accumulators().clone();
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let producer = nats.nats().await;
    let key = NoteKey {
        assignment_id: Uuid::now_v7(),
        seq: 1,
    };
    publish_chunk(&producer, &key, 0, "a").await;
    await_folded(&accumulators, &key, "a").await;

    nats.stop();
    await_not_ready(&readiness).await;

    nats.restart().await;
    await_ready(&readiness).await;

    let producer = nats.nats().await;
    publish_chunk(&producer, &key, 1, "b").await;
    await_folded(&accumulators, &key, "ab").await;

    shutdown.notify_one();
    running
        .await
        .expect("the pod task joins")
        .expect("the pod run returns Ok after a supervised recovery");
    drop(nats);
    db.cleanup().await;
}
