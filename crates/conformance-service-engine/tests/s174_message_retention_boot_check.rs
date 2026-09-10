use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::{
    CreateWidget, SamplePrincipalResolver, WidgetProjector, create_widget, engine_config,
};
use service_engine::error::EngineError;
use service_engine::{Engine, ReadinessHandle};

#[tokio::test]
async fn s174_a_message_retention_shorter_than_the_stream_max_age_is_refused_at_boot() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;

    let mut engine = Engine::boot(
        engine_config("se_s174", "pod-s174").with_message_retention(Duration::from_secs(60)),
        db.app_pool().clone(),
        fabric,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the engine boots; the retention bound is checked when it runs");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector");
    engine
        .register_reaction::<CreateWidget, _, _>("sample-create-widget", create_widget)
        .expect("register a reaction so the inbound loop and its claim retention are wired");
    let readiness = engine.readiness();

    let outcome = engine.run().await;

    let error = outcome.expect_err(
        "a durable replays from the start of its stream, so a message_retention below the \
         stream's max_age would sweep a claim before its message can be redelivered; boot must \
         refuse it rather than risk re-running an unsequenced command",
    );
    assert!(
        matches!(error, EngineError::Config(_)),
        "the refusal is a configuration error, got {error:?}"
    );
    match readiness.snapshot() {
        service_engine::Readiness::Ready => {
            panic!("a pod that refused its retention bound never reports ready")
        }
        service_engine::Readiness::NotReady { reason } => assert_eq!(
            reason,
            service_engine::inbound::REASON_MESSAGE_RETENTION,
            "the retention refusal carries its own reason, not the no-stream label"
        ),
    }

    drop(nats);
    db.cleanup().await;
}
