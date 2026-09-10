use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::{
    PublishedWidget, boot_offer_engine_reconciling, offer_dirty_keys, published_widget, widget_key,
};
use service_engine::Nats;
use sqlx::PgPool;
use uuid::Uuid;

const OBSERVED_WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn s152_a_rebuilt_bucket_is_repopulated_from_the_store_by_the_reconcile() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let first = insert_widget(&pool, tenant, "first").await;
    let second = insert_widget(&pool, tenant, "second").await;

    assert!(
        published_widget(&fabric, first).await.is_none(),
        "the freshly provisioned bucket is empty, as a rebuilt bucket is"
    );

    let engine = boot_offer_engine_reconciling(
        &db,
        nats.nats().await,
        "se_s152",
        "pod-s152",
        Duration::from_millis(150),
    )
    .await;
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());

    await_label(
        &fabric,
        first,
        Some("first"),
        "the reconcile never created the store row missing from the rebuilt bucket",
    )
    .await;
    await_label(
        &fabric,
        second,
        Some("second"),
        "the reconcile never created the second store row missing from the rebuilt bucket",
    )
    .await;

    await_converged(&pool, first).await;
    await_converged(&pool, second).await;

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

async fn insert_widget(pool: &PgPool, tenant: Uuid, label: &str) -> Uuid {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO sample_widget (id, tenant_id, label, closed) VALUES ($1, $2, $3, false)",
    )
    .bind(id)
    .bind(tenant)
    .bind(label)
    .execute(pool)
    .await
    .expect("seed a widget the store owns but the rebuilt bucket has lost");
    id
}

async fn await_label(
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

async fn await_converged(pool: &PgPool, id: Uuid) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        if offer_dirty_keys(pool, id).await == 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the reconcile never cleared the dirty offer key after re-creating the entry"
        );
        tokio::time::sleep(POLL).await;
    }
}
