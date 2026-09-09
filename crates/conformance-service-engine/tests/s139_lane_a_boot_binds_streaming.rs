#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::SamplePrincipal;
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::stream::NoteBody;
use engine_twin::SOON;
use service_engine::error::EngineError;
use service_engine::nats::NatsError;
use service_engine::{Engine, Readiness, ReadinessHandle};

const CHANNEL: &str = "se_s139_streaming_boot";

#[tokio::test]
async fn s139_a_registered_accumulator_without_its_streaming_stream_keeps_the_pod_down() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;

    let mut engine = Engine::<SamplePrincipal>::boot(
        engine_config(CHANNEL, "pod-s139-absent").with_service("s139absent"),
        db.app_pool().clone(),
        fabric,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the engine boots under the low-privilege app role");
    engine
        .register_accumulator(NoteBody)
        .expect("the note-body accumulator enrolls");
    let readiness = engine.readiness();

    let outcome = tokio::time::timeout(SOON, engine.run())
        .await
        .expect("run returns rather than serving over an unbound lane-A stream");

    assert!(
        matches!(outcome, Err(EngineError::Nats(NatsError::NoStream { ref name })) if name == "STREAMING_s139absent"),
        "an absent STREAMING_{{service}} stream fails the boot loud, got {outcome:?}"
    );
    assert_ne!(
        readiness.snapshot(),
        Readiness::Ready,
        "the pod stays out of rotation while its lane-A stream is absent"
    );

    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s139_a_seal_retention_below_the_stream_max_age_fails_the_boot_loud() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    nats.provision_streaming("s139mismatch", Duration::from_secs(600))
        .await;
    let fabric = nats.nats().await;

    let mut engine = Engine::<SamplePrincipal>::boot(
        engine_config(CHANNEL, "pod-s139-mismatch")
            .with_service("s139mismatch")
            .with_seal_retention(Duration::from_secs(60)),
        db.app_pool().clone(),
        fabric,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the engine boots under the low-privilege app role");
    engine
        .register_accumulator(NoteBody)
        .expect("the note-body accumulator enrolls");

    let outcome = tokio::time::timeout(SOON, engine.run())
        .await
        .expect("run returns rather than serving a marker that expires before the stream");

    assert!(
        matches!(
            outcome,
            Err(EngineError::SealRetentionTooShort { ref stream, seal_retention, max_age })
                if stream == "STREAMING_s139mismatch"
                    && seal_retention == Duration::from_secs(60)
                    && max_age == Duration::from_secs(600)
        ),
        "a seal_retention that cannot cover the stream's max_age fails loud, got {outcome:?}"
    );

    drop(nats);
    db.cleanup().await;
}
