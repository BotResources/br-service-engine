use std::sync::Arc;
use std::time::Duration;

use conformance_service_engine::inbound_support::{dead_letter_rows, publish, sample_subscription};
use conformance_service_engine::infra::listener::pool_named;
use conformance_service_engine::infra::{TestDb, TestNats};
use service_engine::inbound::{
    DeadLetters, DispatchCounters, InboundConfig, InboundHealth, InboundLoop, StubDispatch,
    StubPayload, StubVerdict,
};
use uuid::Uuid;

const DURABLE: &str = "se-s172-dead-letter";

fn fast_config() -> InboundConfig {
    InboundConfig {
        ack_wait: Duration::from_secs(1),
        max_ack_pending: 8,
        backoff_base: Duration::from_millis(100),
        backoff_max: Duration::from_millis(200),
    }
}

async fn wait_for_invocations(counters: &DispatchCounters, at_least: usize) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let seen = counters.invocations();
        if seen >= at_least || tokio::time::Instant::now() >= deadline {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn s172_a_dead_letter_write_that_fails_never_terminates_the_frame() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let unwritable = pool_named(&db, db.app_role(), "se_s172_dead_letter_pool").await;
    unwritable.close().await;

    let counters = DispatchCounters::default();
    let dispatch = StubDispatch::new(pool.clone()).with_counters(counters.clone());
    let (health, _rx) = InboundHealth::new();
    let phase_one = InboundLoop::start(
        fabric.clone(),
        vec![sample_subscription(DURABLE)],
        Arc::new(dispatch),
        DeadLetters::new(unwritable),
        fast_config(),
        health,
    )
    .await
    .expect("the inbound loop binds INTEGRATION_CMD and starts");

    let message = Uuid::now_v7();
    publish(
        &fabric,
        message,
        &StubPayload::of(StubVerdict::Terminal),
        None,
    )
    .await;

    let seen = wait_for_invocations(&counters, 3).await;
    assert!(
        seen >= 3,
        "a terminal message whose dead-letter write fails is nak'ed, not terminated, so the \
         broker keeps redelivering it (saw {seen} deliveries)"
    );
    assert_eq!(
        dead_letter_rows(&pool).await,
        0,
        "no dead-letter row exists while the write cannot reach Postgres"
    );

    phase_one.stop();
    phase_one.join().await;

    let (health, _rx) = InboundHealth::new();
    let phase_two = InboundLoop::start(
        fabric.clone(),
        vec![sample_subscription(DURABLE)],
        Arc::new(StubDispatch::new(pool.clone())),
        DeadLetters::new(pool.clone()),
        fast_config(),
        health,
    )
    .await
    .expect("a fresh loop rebinds the same durable and finds the still-pending frame");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while dead_letter_rows(&pool).await < 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the frame the broker kept must dead-letter once Postgres is writable again"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        dead_letter_rows(&pool).await,
        1,
        "exactly one dead-letter row is written and the frame is then terminated, so it settles \
         once and does not redeliver forever"
    );

    phase_two.stop();
    phase_two.join().await;
    drop(nats);
    db.cleanup().await;
}
