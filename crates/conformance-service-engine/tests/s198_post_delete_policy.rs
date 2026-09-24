use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::pipeline::{insert_widget, widget_label};
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{
    DeleteWidget, DeleteWidgetTag, SetWidgetTag, boot_delete_policy_engine,
    boot_offer_trigger_engine,
};
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

async fn widget_tag_count(pool: &PgPool, widget: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM sample_widget_tag WHERE widget_id = $1")
        .bind(widget)
        .fetch_one(pool)
        .await
        .expect("count the widget-tag rows")
}

#[tokio::test]
async fn s198_a_refusing_delete_policy_keeps_the_row() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let pinned = Uuid::now_v7();
    insert_widget(&pool, pinned, tenant, "PINNED-keepme").await;

    let engine = boot_delete_policy_engine(&db, nats.nats().await, "se_s198a", "pod-s198a").await;
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());

    let refused = executor
        .run::<DeleteWidget>(principal.clone(), DeleteWidget { id: pinned })
        .await
        .expect_err("the post-delete policy refuses deleting a pinned widget");
    assert_eq!(
        refused.code(),
        Some("PINNED_DELETE"),
        "the refusal carries the delete policy's reason code"
    );
    assert_eq!(
        widget_label(&pool, pinned).await.as_deref(),
        Some("PINNED-keepme"),
        "the refused delete rolled back, so the row is still present"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s198_an_accepting_delete_policy_lets_the_engine_delete_the_row() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let temp = Uuid::now_v7();
    insert_widget(&pool, temp, tenant, "temporary").await;

    let engine = boot_delete_policy_engine(&db, nats.nats().await, "se_s198b", "pod-s198b").await;
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());

    executor
        .run::<DeleteWidget>(principal.clone(), DeleteWidget { id: temp })
        .await
        .expect("the policy accepts and the engine hard-deletes the row through cx.delete");
    assert_eq!(
        widget_label(&pool, temp).await,
        None,
        "the engine issued the delete, so the row is gone"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s198_a_store_without_delete_refuses_the_call_with_delete_unsupported() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let engine = boot_offer_trigger_engine(
        &db,
        nats.nats().await,
        "se_s198c",
        "pod-s198c",
        Duration::from_secs(300),
    )
    .await;
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());

    let widget = Uuid::now_v7();
    executor
        .run::<SetWidgetTag>(
            principal.clone(),
            SetWidgetTag {
                widget_id: widget,
                tag: "alpha".to_string(),
            },
        )
        .await
        .expect("the tag is created on a store that does not delete");

    let refused = executor
        .run::<DeleteWidgetTag>(principal.clone(), DeleteWidgetTag { widget_id: widget })
        .await
        .expect_err("a store without delete cannot be hard-deleted through cx.delete");
    let described = service_engine::error::describe(&refused);
    assert!(
        described.contains("Persistence::delete"),
        "the failure names the missing store delete, not a generic error: {described}"
    );
    assert_eq!(
        widget_tag_count(&pool, widget).await,
        1,
        "the refused delete wrote nothing, so the tag row is still present"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
