use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{
    MintSecret, PublishedWidget, RelabelWidget, boot_offer_engine_leased, offer_dirty_keys,
    published_widget, widget_key,
};
use service_engine::{Nats, arm_offer_drain};
use sqlx::PgPool;
use uuid::Uuid;

const OBSERVED_WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);
const HOLD_FOR: Duration = Duration::from_millis(1500);

#[tokio::test]
async fn s180_a_leader_frozen_past_its_lease_fails_its_stale_cas_instead_of_regressing() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let lease = Duration::from_millis(600);
    let reconcile = Duration::from_secs(300);
    let a =
        boot_offer_engine_leased(&db, nats.nats().await, "se_s180", "pod-a", reconcile, lease).await;
    let b =
        boot_offer_engine_leased(&db, nats.nats().await, "se_s180", "pod-b", reconcile, lease).await;
    let sa = a.shutdown_handle();
    let sb = b.shutdown_handle();
    let executor = a.mutation_executor();
    let principal = member(&pool, Uuid::now_v7(), tenant).await;

    let gate = arm_offer_drain();

    let ra = tokio::spawn(a.run());
    let rb = tokio::spawn(b.run());

    let id = Uuid::now_v7();
    executor
        .run::<MintSecret>(
            principal.clone(),
            MintSecret {
                id,
                label: "v1".to_string(),
                tenant,
            },
        )
        .await
        .expect("the mint of v1 commits");

    gate.reached().await;

    executor
        .run::<RelabelWidget>(
            principal,
            RelabelWidget {
                id,
                label: "v2".to_string(),
            },
        )
        .await
        .expect("the relabel to v2 commits while the leader is frozen");

    await_label(
        &fabric,
        id,
        Some("v2"),
        "the standby never took the expired lease and published the current truth v2",
    )
    .await;

    gate.release();

    hold_label(
        &fabric,
        id,
        "v2",
        "the woken leader's compare-and-set was on the revision it read at resolve (an empty \
         bucket), so it must conflict; a fresh re-read let its stale v1 overwrite the newer v2",
    )
    .await;
    await_converged(&pool, id).await;

    sa.notify_one();
    sb.notify_one();
    ra.await.expect("join a").expect("a returns Ok");
    rb.await.expect("join b").expect("b returns Ok");
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

async fn hold_label(nats: &Nats, id: Uuid, want: &str, note: &str) {
    let deadline = tokio::time::Instant::now() + HOLD_FOR;
    loop {
        let observed: Option<PublishedWidget> = published_widget(nats, id).await;
        assert_eq!(
            observed.as_ref().map(|w| w.label.as_str()),
            Some(want),
            "{note}"
        );
        if tokio::time::Instant::now() >= deadline {
            return;
        }
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
            "the offer never converged: a dirty key lingered after the contention cleared"
        );
        tokio::time::sleep(POLL).await;
    }
}
