use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::outbox::row_status;
use conformance_service_engine::sample::{delivered_event_ids, stage_outbox_row};
use service_engine::inbound::DeadLetters;
use service_engine::relays::outbox::OutboxRelay;
use sqlx::PgPool;
use uuid::Uuid;

const BACKLOG: usize = 6;
const ACK_TIMEOUT: Duration = Duration::from_millis(200);
const PASSES: usize = 8;
const PUBLISHED_WITHIN: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(100);

#[tokio::test]
async fn s189_a_connected_broker_whose_jetstream_is_unavailable_spends_no_budget_and_delivers_on_return()
 {
    let db = TestDb::fresh().await;
    let mut nats = TestNats::spawn_plain().await;
    let pool = db.app_pool().clone();
    let fabric = nats.nats().await.with_publish_ack_timeout(ACK_TIMEOUT);

    assert!(
        fabric.reachable(),
        "the client is TCP-connected to the plain broker; only JetStream is unavailable"
    );

    let mut staged = Vec::with_capacity(BACKLOG);
    let mut tx = pool.begin().await.expect("open the staging transaction");
    for n in 0..BACKLOG {
        staged.push(stage_outbox_row(&mut tx, &format!("wait-{n}")).await);
    }
    tx.commit().await.expect("commit the backlog");

    let relay = OutboxRelay::new(pool.clone(), fabric.clone())
        .with_dead_letters(DeadLetters::new(pool.clone()));

    for pass_n in 0..PASSES {
        let pass = relay
            .run_once_detailed()
            .await
            .expect("an unanswered pass is a healthy empty pass, never an error");
        assert_eq!(
            pass.picked, 0,
            "pass {pass_n}: a publish the connected broker never answers is no attempt at all"
        );
        assert_eq!(
            pass.retried, 0,
            "pass {pass_n}: no bounded retry is counted"
        );
        assert_eq!(
            pass.dead_lettered, 0,
            "pass {pass_n}: nothing is dead-lettered"
        );
    }

    assert!(
        fabric.reachable(),
        "the TCP connection stayed up throughout the JetStream-unavailable window"
    );
    for id in &staged {
        assert_eq!(
            row_status(&pool, *id).await,
            "PENDING",
            "a committed row waits out the unavailable window, it is never FAILED"
        );
        assert_eq!(
            attempts(&pool, *id).await,
            0,
            "no delivery attempt is spent against a broker that cannot answer"
        );
    }
    assert_eq!(
        dead_letters(&pool).await,
        0,
        "an unavailable JetStream is an outage, not poison: no row is dead-lettered"
    );

    nats.enable_jetstream().await;
    await_all_published(&pool, &staged, &relay).await;

    let mut delivered = delivered_event_ids(&fabric, "se-observer-s189").await;
    delivered.sort();
    let mut expected = staged.clone();
    expected.sort();
    assert_eq!(
        delivered, expected,
        "every waiting row is delivered exactly once once JetStream returns"
    );
    assert_eq!(
        dead_letters(&pool).await,
        0,
        "nothing was abandoned: the window drained clean on return"
    );

    drop(nats);
    db.cleanup().await;
}

async fn attempts(pool: &PgPool, id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT attempts FROM integration_outbox WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read the outbox row attempts")
}

async fn dead_letters(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.dead_letter WHERE source = 'outbox'")
        .fetch_one(pool)
        .await
        .expect("read the outbox dead letters")
}

async fn await_all_published(pool: &PgPool, staged: &[Uuid], relay: &OutboxRelay) {
    let deadline = tokio::time::Instant::now() + PUBLISHED_WITHIN;
    loop {
        let _ = relay.run_once_detailed().await;
        let mut all = true;
        for id in staged {
            if row_status(pool, *id).await != "PUBLISHED" {
                all = false;
                break;
            }
        }
        if all {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the waiting rows never drained after JetStream came back"
        );
        tokio::time::sleep(POLL).await;
    }
}
