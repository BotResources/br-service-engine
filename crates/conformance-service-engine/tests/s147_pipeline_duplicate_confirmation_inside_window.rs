use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_pipeline_engine;
use conformance_service_engine::sample::pipeline::{
    CreateWidget, create_widget_coords, publish_command, wait_for_widget, widget_count,
};
use service_engine::Readiness;
use service_engine::nats::Nats;
use sqlx::PgPool;
use uuid::Uuid;

const CONFIRMATION_SUBJECT: &str = "integration.evt.sample.widget.created.v1";

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

async fn distinct_payload_ids(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(DISTINCT payload->>'event_id') FROM service_engine.integration_outbox \
         WHERE subject = $1",
    )
    .bind(CONFIRMATION_SUBJECT)
    .fetch_one(pool)
    .await
    .expect("count the distinct confirmation envelope ids")
}

async fn stored_in_stream(fabric: &Nats) -> u64 {
    let mut stream = fabric
        .context()
        .get_stream("INTEGRATION_EVT")
        .await
        .expect("the integration event stream exists");
    stream
        .info()
        .await
        .expect("read the stream state")
        .state
        .messages
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
async fn s147_a_duplicate_inside_the_window_re_emits_the_confirmation_once_effect_once() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let engine = boot_pipeline_engine(&db, fabric.clone(), "se_s147", "pod-s147").await;
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
    publish_command(&fabric, &create_widget_coords(), message_id, &command).await;

    assert_eq!(
        wait_for_widget(&pool, tenant, 1).await,
        1,
        "the command committed its effect through the pipeline"
    );

    wait_for(|| async { outbox_rows(&pool).await == 2 }).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        widget_count(&pool, tenant).await,
        1,
        "the duplicate re-ran no effect: the aggregate did nothing, the engine replayed"
    );
    assert_eq!(
        outbox_rows(&pool).await,
        2,
        "the stored confirmation is re-emitted through the outbox on the duplicate claim"
    );
    assert_eq!(
        distinct_payload_ids(&pool).await,
        1,
        "the replay carries the same confirmation envelope id as the first emission"
    );

    wait_for(|| async { published_rows(&pool).await == 2 }).await;
    assert_eq!(
        stored_in_stream(&fabric).await,
        1,
        "inside the duplicate window the broker collapses the re-emitted confirmation to one \
         stored message, so a producer that lost its confirmation is not double-confirmed"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
