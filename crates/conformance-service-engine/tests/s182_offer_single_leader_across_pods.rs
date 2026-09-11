use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{
    MintSecret, PublishedWidget, boot_offer_engine_reconciling, published_widget, widget_key,
};
use service_engine::Nats;
use sqlx::PgPool;
use uuid::Uuid;

const OBSERVED_WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);
const RELAY_SLOT: &str = "relay:widget_v1";

#[tokio::test]
async fn s182_two_pods_share_a_single_offer_leader_lease() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let a = boot_offer_engine_reconciling(
        &db,
        nats.nats().await,
        "se_s182",
        "pod-a",
        Duration::from_secs(300),
    )
    .await;
    let b = boot_offer_engine_reconciling(
        &db,
        nats.nats().await,
        "se_s182",
        "pod-b",
        Duration::from_secs(300),
    )
    .await;
    let sa = a.shutdown_handle();
    let sb = b.shutdown_handle();
    let executor = a.mutation_executor();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;
    let ra = tokio::spawn(a.run());
    let rb = tokio::spawn(b.run());

    let id = Uuid::now_v7();
    executor
        .run::<MintSecret>(
            principal,
            MintSecret {
                id,
                label: "shared".to_string(),
                tenant,
            },
        )
        .await
        .expect("the mint commits");

    await_label(&fabric, id, Some("shared")).await;

    assert_eq!(
        leader_rows(&pool, RELAY_SLOT).await,
        1,
        "two pods draining an offer over several beats must share one fixed-slot lease; a lease \
         claimed anew per beat would leave a row per beat and let both pods drain at once"
    );

    sa.notify_one();
    sb.notify_one();
    ra.await.expect("join a").expect("a returns Ok");
    rb.await.expect("join b").expect("b returns Ok");
    drop(nats);
    db.cleanup().await;
}

async fn leader_rows(pool: &PgPool, name: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.leader_slot WHERE name = $1")
        .bind(name)
        .fetch_one(pool)
        .await
        .expect("count the offer relay's leader-slot rows")
}

async fn await_label(nats: &Nats, id: Uuid, want: Option<&str>) {
    let _ = widget_key(id).expect("a valid offer key");
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        let observed: Option<PublishedWidget> = published_widget(nats, id).await;
        if observed.as_ref().map(|w| w.label.as_str()) == want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the shared offer never converged in the bucket"
        );
        tokio::time::sleep(POLL).await;
    }
}
