use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{
    DeleteWidget, MintSecret, PublishedWidget, boot_offer_engine_reconciling, offer_dirty_keys,
    published_widget, widget_key,
};
use service_engine::Nats;
use sqlx::PgPool;
use uuid::Uuid;

const OBSERVED_WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn s180_cx_delete_retracts_an_offered_row_before_the_reconcile() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let engine = boot_offer_engine_reconciling(
        &db,
        nats.nats().await,
        "se_s180",
        "pod-s180",
        Duration::from_secs(300),
    )
    .await;
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let running = tokio::spawn(engine.run());

    let id = Uuid::now_v7();
    executor
        .run::<MintSecret>(
            principal.clone(),
            MintSecret {
                id,
                label: "offered".to_string(),
                tenant,
            },
        )
        .await
        .expect("the mint commits");
    await_label(&fabric, id, Some("offered"), "the widget was never offered").await;

    executor
        .run::<DeleteWidget>(principal, DeleteWidget { id })
        .await
        .expect("the delete commits");

    await_label(
        &fabric,
        id,
        None,
        "deleting the row through cx.delete never retracted the offer; with a 300 s reconcile the \
         only path to a retract is the dirty key the engine must stage in the delete",
    )
    .await;
    await_converged(&pool, id).await;

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

async fn await_label(nats: &Nats, id: Uuid, want: Option<&str>, note: &str) {
    let _ = widget_key(id).expect("a valid offer key");
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        let observed: Option<PublishedWidget> = published_widget(nats, id).await;
        if observed.as_ref().map(|w| w.label.as_str()) == want {
            return;
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
            "the leader never cleared the dirty key after the retract landed"
        );
        tokio::time::sleep(POLL).await;
    }
}
