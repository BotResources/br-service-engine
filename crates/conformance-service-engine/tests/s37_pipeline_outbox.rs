use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_pipeline_engine;
use conformance_service_engine::sample::pipeline::{
    CreateWidget, create_widget_coords, publish_command, wait_for_widget,
};
use service_engine::OutboxRelay;
use sqlx::PgPool;
use uuid::Uuid;

async fn outbox_status(pool: &PgPool, subject_like: &str) -> Option<String> {
    sqlx::query_scalar("SELECT status FROM integration_outbox WHERE subject LIKE $1 LIMIT 1")
        .bind(subject_like)
        .fetch_optional(pool)
        .await
        .expect("read the outbox status")
}

#[tokio::test]
async fn s37_an_emitted_event_is_staged_in_the_write_tx_and_published_by_the_leader_relay() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let engine = boot_pipeline_engine(&db, fabric.clone(), "se_s37", "pod-s37").await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while readiness.snapshot() != br_util_axum_readiness::Readiness::Ready {
        assert!(tokio::time::Instant::now() < deadline, "never ready");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let widget = Uuid::now_v7();
    publish_command(
        &fabric,
        &create_widget_coords(),
        Uuid::now_v7(),
        &CreateWidget {
            id: widget,
            tenant,
            label: "emits".to_string(),
        },
    )
    .await;
    assert_eq!(wait_for_widget(&pool, tenant, 1).await, 1);

    assert_eq!(
        outbox_status(&pool, "%widget.created%").await.as_deref(),
        Some("PENDING"),
        "the emitted event is an outbox row committed with the state change",
    );

    let relay = OutboxRelay::new(pool.clone(), fabric.clone());
    relay
        .run_once_detailed()
        .await
        .expect("the leader relay drains the outbox");
    assert_eq!(
        outbox_status(&pool, "%widget.created%").await.as_deref(),
        Some("PUBLISHED"),
        "the leader relay published the staged event",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
