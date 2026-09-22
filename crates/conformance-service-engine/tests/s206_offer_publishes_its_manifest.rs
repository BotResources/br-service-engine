use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{MintSecret, WIDGET_OFFER_PREFIX, read_offer_manifest};
use service_engine::{Nats, OfferManifest};
use uuid::Uuid;

const OBSERVED_WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn s206_the_offer_leader_writes_its_manifest_at_reconcile() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let engine = conformance_service_engine::sample::boot_offer_engine(
        &db,
        nats.nats().await,
        "se_s206",
        "pod-s206",
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
        .expect("the mint commits and the offer reconciles");

    let manifest = await_manifest(&fabric, WIDGET_OFFER_PREFIX).await;
    assert_eq!(
        manifest,
        Some(OfferManifest {
            prefix: WIDGET_OFFER_PREFIX.to_string(),
            version: 1,
        }),
        "the offer publishes its prefix and version under the manifest key"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

async fn await_manifest(nats: &Nats, prefix: &str) -> Option<OfferManifest> {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        let found = read_offer_manifest(nats, prefix).await;
        if found.is_some() || tokio::time::Instant::now() >= deadline {
            return found;
        }
        tokio::time::sleep(POLL).await;
    }
}
