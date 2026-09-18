use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_pipeline_engine;
use conformance_service_engine::sample::pipeline::{
    CreateWidget, create_widget_coords, publish_command, wait_for_widget, widget_count,
};
use service_engine::Readiness;
use sqlx::PgPool;
use uuid::Uuid;

async fn outbox_rows(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.integration_outbox")
        .fetch_one(pool)
        .await
        .expect("count the outbox rows")
}

async fn published_rows(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM service_engine.integration_outbox WHERE status = 'PUBLISHED'",
    )
    .fetch_one(pool)
    .await
    .expect("count the published outbox rows")
}

async fn wait_ready(readiness: &service_engine::ReadinessHandle) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while readiness.snapshot() != Readiness::Ready {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the pipeline engine never became ready"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for<F, Fut>(mut probe: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if probe().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the awaited condition never held"
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

#[tokio::test]
async fn s148_a_duplicate_outside_the_window_re_emits_from_the_durable_claim_effect_once() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let engine = boot_pipeline_engine(&db, fabric.clone(), "se_s148", "pod-s148").await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());
    wait_ready(&readiness).await;

    let widget = Uuid::now_v7();
    let message_id = Uuid::now_v7();
    let command = CreateWidget {
        id: widget,
        tenant,
        label: "confirm".to_string(),
    };

    publish_command(&fabric, &create_widget_coords(), message_id, &command).await;
    assert_eq!(
        wait_for_widget(&pool, tenant, 1).await,
        1,
        "the first command committed its effect"
    );
    wait_for(|| async { published_rows(&pool).await == 1 }).await;

    publish_command(&fabric, &create_widget_coords(), message_id, &command).await;

    wait_for(|| async { outbox_rows(&pool).await == 2 }).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        widget_count(&pool, tenant).await,
        1,
        "the duplicate ran no second effect after the original confirmation had already left"
    );
    assert_eq!(
        outbox_rows(&pool).await,
        2,
        "the receiver's durable claim re-emits the confirmation even once the original emission \
         has been fully published and forgotten by the broker's duplicate window"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
