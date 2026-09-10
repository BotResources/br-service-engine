use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{
    MintSecret, PublishedWidget, boot_offer_engine_reconciling, offer_dirty_keys, published_widget,
    seed_bucket, widget_key,
};
use service_engine::Nats;
use sqlx::PgPool;
use uuid::Uuid;

const OBSERVED_WITHIN: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(50);
const HAMMER_FOR: Duration = Duration::from_secs(2);
const HAMMER_EVERY: Duration = Duration::from_millis(15);

#[tokio::test]
async fn s153_a_leader_that_loses_the_cas_re_marks_the_key_and_converges_once_contention_clears() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await;

    let tenant = Uuid::now_v7();
    let engine = boot_offer_engine_reconciling(
        &db,
        nats.nats().await,
        "se_s148",
        "pod-s148",
        Duration::from_millis(120),
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
                label: "truth".to_string(),
                tenant,
            },
        )
        .await
        .expect("the mint commits");
    await_label(&fabric, id, Some("truth"), "the widget was never offered").await;

    let stop = Arc::new(AtomicBool::new(false));
    let hammer = {
        let hammer_nats = nats.nats().await;
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                seed_bucket(&hammer_nats, id, &format!("contender-{n}")).await;
                n += 1;
                tokio::time::sleep(HAMMER_EVERY).await;
            }
        })
    };

    tokio::time::sleep(HAMMER_FOR).await;
    stop.store(true, Ordering::Relaxed);
    hammer.await.expect("the contending writer joins");

    await_label(
        &fabric,
        id,
        Some("truth"),
        "once the concurrent writer stopped, the leader never won the compare-and-set back to the \
         store's truth: a lost CAS must leave the key dirty and converge on the next drain",
    )
    .await;
    await_converged(&pool, id).await;

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

async fn await_converged(pool: &PgPool, id: Uuid) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        if offer_dirty_keys(pool, id).await == 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the leader never cleared the dirty key after the CAS finally landed"
        );
        tokio::time::sleep(POLL).await;
    }
}
