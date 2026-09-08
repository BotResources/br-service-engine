use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{
    MintSecret, PublishedWidget, boot_offer_engine_reconciling, published_widget, seed_bucket,
    widget_key,
};
use service_engine::Nats;
use uuid::Uuid;

const OBSERVED_WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn s51_a_stable_leader_repairs_out_of_band_drift_on_its_periodic_reconcile() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let engine = boot_offer_engine_reconciling(
        &db,
        nats.nats().await,
        "se_s51",
        "pod-s51",
        Duration::from_millis(150),
    )
    .await;
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

    await_label(
        &fabric,
        id,
        Some("alpha"),
        "the leader never put the freshly minted widget",
    )
    .await;

    seed_bucket(&fabric, id, "drifted-out-of-band").await;

    await_label(
        &fabric,
        id,
        Some("alpha"),
        "a stable leader that never restarts never re-reconciled the drifted bucket back to the store",
    )
    .await;

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
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
