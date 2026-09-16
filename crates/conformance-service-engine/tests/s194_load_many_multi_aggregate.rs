//! `load_many` writes several aggregates of one noun in a single pipeline
//! transaction under the engine's deterministic lock order: after the commit
//! both are visible, and a refusal leaves neither changed — the write is atomic
//! and needs no global advisory lock.

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_pipeline_engine;
use conformance_service_engine::sample::pipeline::{RelabelBoth, insert_widget, widget_label};
use conformance_service_engine::sample::principal::SamplePrincipal;
use conformance_service_engine::sample::render::member;
use uuid::Uuid;

#[tokio::test]
async fn s194_two_aggregates_relabelled_in_one_tx_are_both_visible_after_commit_and_neither_before()
{
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal: SamplePrincipal = member(&pool, Uuid::now_v7(), tenant).await;
    let first = Uuid::now_v7();
    let second = Uuid::now_v7();
    insert_widget(&pool, first, tenant, "before").await;
    insert_widget(&pool, second, tenant, "before").await;

    let engine = boot_pipeline_engine(&db, nats.nats().await, "se_s194", "pod-s194").await;
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());

    // Neither is relabelled before the mutation commits.
    assert_eq!(widget_label(&pool, first).await.as_deref(), Some("before"));
    assert_eq!(widget_label(&pool, second).await.as_deref(), Some("before"));

    executor
        .run::<RelabelBoth>(
            principal.clone(),
            RelabelBoth {
                first,
                second,
                label: "after".to_string(),
            },
        )
        .await
        .expect("both aggregates load, mutate and save in one transaction");

    // Both are visible after the commit.
    assert_eq!(widget_label(&pool, first).await.as_deref(), Some("after"));
    assert_eq!(widget_label(&pool, second).await.as_deref(), Some("after"));

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s194_a_relabel_both_over_a_missing_aggregate_commits_nothing() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal: SamplePrincipal = member(&pool, Uuid::now_v7(), tenant).await;
    let present = Uuid::now_v7();
    let absent = Uuid::now_v7();
    insert_widget(&pool, present, tenant, "before").await;

    let engine = boot_pipeline_engine(&db, nats.nats().await, "se_s194b", "pod-s194b").await;
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());

    executor
        .run::<RelabelBoth>(
            principal.clone(),
            RelabelBoth {
                first: present,
                second: absent,
                label: "after".to_string(),
            },
        )
        .await
        .expect_err("only one aggregate exists, so the handler refuses and the tx rolls back");

    // The present aggregate keeps its label: the partial write did not commit.
    assert_eq!(
        widget_label(&pool, present).await.as_deref(),
        Some("before")
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
