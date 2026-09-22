use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::{
    CreateWidget, SamplePrincipalResolver, WidgetProjector, create_widget, engine_config,
};
use service_engine::{Engine, Readiness, ReadinessHandle};

#[tokio::test]
async fn s228_a_message_retention_override_above_the_stream_max_age_boots_ready() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision_integration(Duration::from_secs(90 * 60))
        .await;
    let fabric = nats.nats().await;

    let override_retention = Duration::from_secs(2 * 60 * 60);
    let mut engine = Engine::boot(
        engine_config("se_s228", "pod-s228").with_message_retention(override_retention),
        db.app_pool().clone(),
        fabric,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the engine boots");
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
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if readiness.snapshot() == Readiness::Ready {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the stream max_age (5400s) exceeds DEFAULT_MESSAGE_RETENTION (3600s), so only the \
             with_message_retention override (7200s) covers it; if the override were a no-op the \
             default would be refused like s174. readiness never reached ready. last reason: {:?}",
            readiness.snapshot(),
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
