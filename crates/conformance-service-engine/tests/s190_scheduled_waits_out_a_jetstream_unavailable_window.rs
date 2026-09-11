use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use service_engine::housekeeping::scheduled_message::fire_due;
use service_engine::inbound::DeadLetters;
use sqlx::PgPool;
use uuid::Uuid;

const SUBJECT: &str = "integration.cmd.s190.work.do.v1";
const BACKLOG: usize = 4;
const BUDGET: u32 = 3;
const BATCH: i64 = 128;
const ACK_TIMEOUT: Duration = Duration::from_millis(200);
const ROUNDS: usize = 6;

#[tokio::test]
async fn s190_scheduled_rows_wait_out_a_jetstream_unavailable_window_then_fire_once() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await.with_publish_ack_timeout(ACK_TIMEOUT);
    let dead_letters = DeadLetters::new(pool.clone());

    let sub_client = async_nats::connect(&nats.url())
        .await
        .expect("dial the broker for a plain responder");
    let _sink = sub_client
        .subscribe("integration.cmd.>")
        .await
        .expect("a non-jetstream responder keeps the server from answering `no responders`");
    sub_client.flush().await.expect("flush the subscription");

    assert!(
        fabric.reachable(),
        "the client is TCP-connected; the command stream is simply not there to answer yet"
    );

    let mut staged = Vec::with_capacity(BACKLOG);
    for _ in 0..BACKLOG {
        staged.push(schedule(&pool, SUBJECT).await);
    }

    for round_n in 0..ROUNDS {
        let round = fire_due(&pool, &fabric, &dead_letters, BATCH, BUDGET)
            .await
            .expect("an unanswered round never errors");
        assert_eq!(
            round.fired, 0,
            "round {round_n}: a publish the connected broker never answers fires nothing"
        );
        assert_eq!(round.retried, 0, "round {round_n}: no attempt is counted");
        assert_eq!(
            round.dead_lettered, 0,
            "round {round_n}: nothing is dead-lettered"
        );
    }

    assert!(
        fabric.reachable(),
        "the TCP connection stayed up throughout the JetStream-unavailable window"
    );
    assert_eq!(
        pending(&pool).await,
        BACKLOG as i64,
        "every scheduled row waits, none fired and none dropped"
    );
    for id in &staged {
        assert_eq!(
            attempts(&pool, *id).await,
            0,
            "no delivery attempt is spent against a broker that cannot answer"
        );
    }
    assert_eq!(
        scheduled_dead_letters(&pool).await,
        0,
        "an unavailable JetStream is an outage, not poison: no scheduled row is dead-lettered"
    );

    nats.provision().await;

    let mut fired = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while pending(&pool).await > 0 {
        let round = fire_due(&pool, &fabric, &dead_letters, BATCH, BUDGET)
            .await
            .expect("a reachable round fires");
        fired += round.fired;
        assert_eq!(
            round.dead_lettered, 0,
            "a working broker dead-letters nothing"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "the waiting scheduled rows never fired after the stream was created"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        fired, BACKLOG,
        "every waiting row fired exactly once once the stream was there to answer"
    );

    drop(nats);
    db.cleanup().await;
}

async fn schedule(pool: &PgPool, subject: &str) -> Uuid {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO service_engine.scheduled_message \
           (id, at, source, subject, message_id, payload) \
         VALUES ($1, now(), 'test', $2, $3, $4)",
    )
    .bind(id)
    .bind(subject)
    .bind(Uuid::now_v7())
    .bind(b"{}".as_slice())
    .execute(pool)
    .await
    .expect("stage a scheduled message");
    id
}

async fn pending(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.scheduled_message")
        .fetch_one(pool)
        .await
        .expect("count the scheduled rows")
}

async fn attempts(pool: &PgPool, id: Uuid) -> i32 {
    sqlx::query_scalar("SELECT attempts FROM service_engine.scheduled_message WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read the scheduled row attempts")
}

async fn scheduled_dead_letters(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.dead_letter WHERE source = 'scheduled'")
        .fetch_one(pool)
        .await
        .expect("read the scheduled dead letters")
}
