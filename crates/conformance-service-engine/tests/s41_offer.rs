use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{
    CloseWidget, MintSecret, MintThenReject, PublishedWidget, boot_offer_engine, offer_dirty_keys,
    published_widget, seed_bucket, widget_key,
};
use service_engine::Nats;
use sqlx::PgPool;
use uuid::Uuid;

const OBSERVED_WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn s41_a_saved_noun_is_offered_by_the_leader_after_the_commit() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let engine = boot_offer_engine(&db, nats.nats().await, "se_s41a", "pod-s41a").await;
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let running = tokio::spawn(engine.run());

    let id = Uuid::now_v7();
    executor
        .run::<MintSecret>(
            principal,
            MintSecret {
                id,
                label: "alpha".to_string(),
                tenant,
            },
        )
        .await
        .expect("the mint commits, staging the offer dirty key in the same transaction");

    let offered = await_offer(
        &fabric,
        id,
        Some("alpha"),
        "the leader never put the saved widget into the bucket",
    )
    .await;
    assert_eq!(offered.expect("a published widget").id, id);

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s41_a_rolled_back_mutation_leaves_no_dirty_key_and_nothing_offered() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let engine = boot_offer_engine(&db, nats.nats().await, "se_s41b", "pod-s41b").await;
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let running = tokio::spawn(engine.run());

    let id = Uuid::now_v7();
    let refused = executor
        .run::<MintThenReject>(
            principal,
            MintThenReject {
                id,
                tenant,
                label: "phantom".to_string(),
            },
        )
        .await;
    assert!(refused.is_err(), "the mutation rolls its transaction back");

    assert_eq!(
        widget_rows(&pool, id).await,
        0,
        "the aggregate write rolled back with the mutation"
    );
    assert_eq!(
        offer_dirty_keys(&pool, id).await,
        0,
        "the dirty offer key is committed with the write, so a rollback leaves none behind"
    );
    // Give the beat several ticks; nothing was committed, so nothing can ever be offered.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        published_widget(&fabric, id).await.is_none(),
        "a rolled-back write is never offered"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s41_a_noun_that_stops_being_offerable_is_retracted() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let engine = boot_offer_engine(&db, nats.nats().await, "se_s41c", "pod-s41c").await;
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let mint_principal = member(&pool, Uuid::now_v7(), tenant).await;
    let close_principal = member(&pool, Uuid::now_v7(), tenant).await;
    let running = tokio::spawn(engine.run());

    let id = Uuid::now_v7();
    executor
        .run::<MintSecret>(
            mint_principal,
            MintSecret {
                id,
                label: "beta".to_string(),
                tenant,
            },
        )
        .await
        .expect("the mint commits");
    await_offer(&fabric, id, Some("beta"), "the widget was never offered").await;

    executor
        .run::<CloseWidget>(close_principal, CloseWidget { id })
        .await
        .expect("the close commits");
    await_offer(
        &fabric,
        id,
        None,
        "closing the widget never retracted it from the bucket",
    )
    .await;

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s41_boot_reconcile_repairs_a_bucket_that_drifted() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let kept = Uuid::now_v7();
    let orphan = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO sample_widget (id, tenant_id, label, closed) VALUES ($1, $2, $3, false)",
    )
    .bind(kept)
    .bind(tenant)
    .bind("correct")
    .execute(&pool)
    .await
    .expect("a widget the store still owns");
    seed_bucket(&fabric, kept, "stale-label").await;
    seed_bucket(&fabric, orphan, "orphan").await;

    let engine = boot_offer_engine(&db, nats.nats().await, "se_s41d", "pod-s41d").await;
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());

    await_offer(
        &fabric,
        kept,
        Some("correct"),
        "the reconcile never re-put the stale offer from the store",
    )
    .await;
    await_offer(
        &fabric,
        orphan,
        None,
        "the reconcile never retracted the orphan the store does not own",
    )
    .await;

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

async fn await_offer(
    nats: &Nats,
    id: Uuid,
    want: Option<&str>,
    note: &str,
) -> Option<PublishedWidget> {
    let _ = widget_key(id).expect("a valid offer key");
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        let observed = published_widget(nats, id).await;
        if observed.as_ref().map(|w| w.label.as_str()) == want {
            return observed;
        }
        assert!(tokio::time::Instant::now() < deadline, "{note}");
        tokio::time::sleep(POLL).await;
    }
}

async fn widget_rows(pool: &PgPool, id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM sample_widget WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("count the widget rows")
}
