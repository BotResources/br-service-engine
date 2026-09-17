use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::pipeline::insert_widget;
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{
    SetWidgetTag, boot_offer_trigger_engine, published_widget,
};
use service_engine::Nats;
use uuid::Uuid;

const OBSERVED_WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);

async fn await_label(nats: &Nats, id: Uuid, want: Option<&str>, note: &str) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        let observed = published_widget(nats, id).await;
        if observed.as_ref().map(|w| w.label.as_str()) == want {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "{note}");
        tokio::time::sleep(POLL).await;
    }
}

#[tokio::test]
async fn s199_saving_a_trigger_aggregate_republishes_the_offer_from_the_store() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let widget = Uuid::now_v7();
    insert_widget(&pool, widget, tenant, "orig").await;

    let engine = boot_offer_trigger_engine(
        &db,
        nats.nats().await,
        "se_s199",
        "pod-s199",
        Duration::from_secs(300),
    )
    .await;
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());

    await_label(
        &fabric,
        widget,
        Some("orig"),
        "the boot reconcile never offered the store's widget",
    )
    .await;

    sqlx::query("UPDATE sample_widget SET label = 'changed' WHERE id = $1")
        .bind(widget)
        .execute(&pool)
        .await
        .expect("mutate the store row out of band, so only a trigger can republish it");

    executor
        .run::<SetWidgetTag>(
            principal.clone(),
            SetWidgetTag {
                widget_id: widget,
                tag: "hot".to_string(),
            },
        )
        .await
        .expect("the tag save dirties the widget offer through its trigger");

    await_label(
        &fabric,
        widget,
        Some("changed"),
        "saving the trigger never republished the offer from the store's current state; \
         the periodic reconcile is 300s away, so only the trigger could have",
    )
    .await;

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
