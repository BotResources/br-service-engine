use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::{
    CreateWidget, SamplePrincipalResolver, WidgetProjector, create_widget, engine_config,
};
use service_engine::error::EngineError;
use service_engine::{Engine, ReadinessHandle};

#[tokio::test]
async fn s227_an_unbounded_bound_stream_is_refused_at_boot_with_the_retention_reason() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision_integration(Duration::ZERO).await;
    let fabric = nats.nats().await;

    let mut engine = Engine::boot(
        engine_config("se_s227", "pod-s227").with_message_retention(Duration::from_secs(3600)),
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
        .expect("register a reaction so the inbound loop binds its stream");
    let readiness = engine.readiness();

    let error = engine.run().await.expect_err(
        "a durable replays from the start of its stream; an unlimited max_age can never be \
         covered by any finite message_retention, so a swept claim could still be redelivered \
         and re-run its effect. Boot must refuse an unbounded bound stream",
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
            "the unbounded-stream refusal carries the message-retention reason"
        ),
    }

    drop(nats);
    db.cleanup().await;
}
