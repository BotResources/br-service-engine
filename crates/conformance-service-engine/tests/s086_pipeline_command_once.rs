use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_pipeline_engine;
use conformance_service_engine::sample::pipeline::{
    CreateWidget, create_widget_coords, publish_command, wait_for_widget, widget_count,
};
use uuid::Uuid;

#[tokio::test]
async fn s086_a_booted_engine_consumes_a_command_through_the_pipeline_exactly_once() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let engine = boot_pipeline_engine(&db, fabric.clone(), "se_s086", "pod-s086").await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    while readiness.snapshot() != service_engine::Readiness::Ready {
        assert!(
            tokio::time::Instant::now() < deadline,
            "engine never became ready"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    let widget = Uuid::now_v7();
    let message_id = Uuid::now_v7();
    let command = CreateWidget {
        id: widget,
        tenant,
        label: "from-nats".to_string(),
    };
    publish_command(&fabric, &create_widget_coords(), message_id, &command).await;
    publish_command(&fabric, &create_widget_coords(), message_id, &command).await;

    assert_eq!(
        wait_for_widget(&pool, tenant, 1).await,
        1,
        "the booted engine consumed the command through the real pipeline (no test-support seam)",
    );
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert_eq!(
        widget_count(&pool, tenant).await,
        1,
        "a redelivery of the same message id is a claimed no-op, so the effect is exactly once",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
