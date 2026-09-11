use std::time::{Duration, Instant};

use conformance_service_engine::infra::{TestDb, TestNats};
use service_engine::housekeeping::scheduled_message::fire_due;
use service_engine::inbound::{DeadLetterSource, DeadLetters};
use service_engine::nats::Nats;
use sqlx::PgPool;
use uuid::Uuid;

const SUBJECT: &str = "integration.cmd.s184.work.do.v1";
const BUDGET: u32 = 8;

async fn schedule(pool: &PgPool) -> Uuid {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO service_engine.scheduled_message \
           (id, at, source, subject, message_id, payload) \
         VALUES ($1, now(), 'test', $2, $3, $4)",
    )
    .bind(id)
    .bind(SUBJECT)
    .bind(Uuid::now_v7())
    .bind(b"{}".as_slice())
    .execute(pool)
    .await
    .expect("stage a scheduled message");
    id
}

async fn attempts(pool: &PgPool, id: Uuid) -> i32 {
    sqlx::query_scalar("SELECT attempts FROM service_engine.scheduled_message WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read the row's attempt count")
}

async fn pending(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.scheduled_message")
        .fetch_one(pool)
        .await
        .expect("count the scheduled rows")
}

async fn wait_for_reachable(nats: &Nats, reachable: bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while nats.reachable() != reachable {
        assert!(
            Instant::now() < deadline,
            "the client never observed the broker reach reachable={reachable}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn s184_a_scheduled_row_survives_a_broker_outage_without_spending_its_budget() {
    let db = TestDb::fresh().await;
    let mut nats = TestNats::spawn().await;
    nats.provision().await;
    let engine_nats = nats.nats().await;
    let pool = db.app_pool().clone();
    let dead_letters = DeadLetters::new(pool.clone());

    let id = schedule(&pool).await;

    nats.stop();
    wait_for_reachable(&engine_nats, false).await;

    for _ in 0..(BUDGET + 2) {
        let round = fire_due(&pool, &engine_nats, &dead_letters, 128, BUDGET)
            .await
            .expect("fire_due is a no-op under an outage, never an error");
        assert_eq!(
            (round.fired, round.dead_lettered, round.retried),
            (0, 0, 0),
            "a due row is neither fired, dead-lettered nor retried while the broker is unreachable",
        );
    }
    assert_eq!(
        attempts(&pool, id).await,
        0,
        "no failure observed against an unreachable broker is counted, so more beats than the \
         whole delivery budget leave the budget untouched",
    );
    assert_eq!(
        pending(&pool).await,
        1,
        "the row stays due through the outage rather than being deleted or dead-lettered",
    );
    assert!(
        dead_letters
            .list(Some(DeadLetterSource::Scheduled), 16)
            .await
            .expect("read the dead-letter table")
            .is_empty(),
        "an outage is not poison, so it produces no dead-letter row",
    );

    nats.restart().await;
    wait_for_reachable(&engine_nats, true).await;

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut fired = 0;
    while fired == 0 {
        assert!(
            Instant::now() < deadline,
            "the row never fired after the broker returned"
        );
        fired = fire_due(&pool, &engine_nats, &dead_letters, 128, BUDGET)
            .await
            .expect("fire_due fires the due row once the broker is back")
            .fired;
    }
    assert_eq!(fired, 1, "the row fires exactly once on return");
    assert_eq!(
        pending(&pool).await,
        0,
        "the fired row leaves the queue; it waited out the outage and then delivered",
    );

    drop(nats);
    db.cleanup().await;
}
