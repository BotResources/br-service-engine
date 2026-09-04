use std::sync::Arc;
use std::time::Duration;

use br_util_nats_fabric::{DEFAULT_MAX_MESSAGES, OutboxRelay};
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::{
    FailingSampleRelay, RecordingTransport, RowClaimSampleRelay, delivered_event_ids,
    stage_outbox_row, staged_impacts,
};
use service_engine::housekeeping::health::RelayCondition;
use service_engine::housekeeping::relay::RelayRuntime;
use service_engine::impact::{Dims, Impact};
use service_engine::name::{PodId, RelayName};
use service_engine::relays::outbox::FabricOutboxRelay;
use service_engine::transport::ImpactTransport;
use sqlx::PgPool;
use uuid::Uuid;

const ROWS: usize = 40;
const OUTBOX: RelayName = RelayName::from_static("integration_outbox");

#[tokio::test]
async fn s15_row_claim_never_drains_the_same_row_concurrently() {
    let db = TestDb::fresh().await;
    seed_rows(db.app_pool(), ROWS).await;

    let one = db.pool_as(db.app_role()).await.expect("a pool per pod");
    let two = db.pool_as(db.app_role()).await.expect("a pool per pod");
    let mut first = runtime("se-relay-0");
    let mut second = runtime("se-relay-1");
    first
        .register(RowClaimSampleRelay::new(
            RelayName::from_static("sample_rows"),
            Duration::from_millis(15),
        ))
        .expect("the relay registers");
    second
        .register(RowClaimSampleRelay::new(
            RelayName::from_static("sample_rows"),
            Duration::from_millis(15),
        ))
        .expect("the relay registers");

    tokio::join!(
        beat_for(&mut first, &one, 24, Duration::from_millis(7)),
        beat_for(&mut second, &two, 24, Duration::from_millis(11)),
    );

    let claims: i64 = scalar(db.app_pool(), "SELECT count(*) FROM sample_relay_claim").await;
    let distinct: i64 = scalar(
        db.app_pool(),
        "SELECT count(DISTINCT row_id) FROM sample_relay_claim",
    )
    .await;
    let unclaimed: i64 = scalar(
        db.app_pool(),
        "SELECT count(*) FROM sample_relay_row WHERE claimed_at IS NULL",
    )
    .await;
    assert_eq!(unclaimed, 0, "two skewed runtimes drain every row");
    assert_eq!(
        claims, distinct,
        "FOR UPDATE SKIP LOCKED must make a second claim of one row impossible"
    );
    assert_eq!(distinct, ROWS as i64);
    let pods: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT claimed_by FROM sample_relay_claim ORDER BY 1")
            .fetch_all(db.app_pool())
            .await
            .expect("read which pods claimed a row");
    assert_eq!(
        pods.len(),
        2,
        "both runtimes claimed rows, so they really raced"
    );
    assert!(first.health().borrow().is_healthy() && second.health().borrow().is_healthy());

    one.close().await;
    two.close().await;
    db.cleanup().await;
}

#[tokio::test]
async fn s15_a_row_staged_with_an_impact_is_published_within_one_window() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;

    let relay = Arc::new(FabricOutboxRelay::hosting(
        OUTBOX,
        OutboxRelay::new(db.app_pool().clone(), fabric.clone()),
        DEFAULT_MAX_MESSAGES,
    ));
    let mut engine = runtime("se-relay-0");
    engine
        .register_erased(relay.clone())
        .expect("the relay registers");

    let mut tx = db.app_pool().begin().await.expect("the write transaction");
    let staged = stage_outbox_row(&mut tx, "within-one-window").await;
    RecordingTransport
        .stage_in(
            &mut tx,
            &[
                Impact::resource::<conformance_service_engine::sample::Assignment>(
                    &Uuid::now_v7(),
                    Dims::ALL,
                )
                .expect("the impact encodes its key"),
            ],
        )
        .await
        .expect("the impact rides in the same transaction as the outbox row");
    tx.commit().await.expect("commit");

    let round = engine.after_pass(db.app_pool()).await;
    assert_eq!(round.ran, 1);
    assert_eq!(round.rows, 1, "the row staged with the impact left at once");
    assert_eq!(staged_impacts(db.app_pool()).await.len(), 1);
    assert_eq!(
        delivered_event_ids(&fabric, "se-observer-window").await,
        vec![staged]
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s15_a_crash_between_publish_and_status_transition_republishes_once() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;

    let relay = Arc::new(FabricOutboxRelay::hosting(
        OUTBOX,
        OutboxRelay::new(db.app_pool().clone(), fabric.clone()),
        DEFAULT_MAX_MESSAGES,
    ));
    let mut engine = runtime("se-relay-0");
    engine
        .register_erased(relay.clone())
        .expect("the relay registers");

    let mut tx = db.app_pool().begin().await.expect("the write transaction");
    let staged = stage_outbox_row(&mut tx, "crash-replay").await;
    tx.commit().await.expect("commit");

    assert_eq!(engine.beat(db.app_pool()).await.rows, 1);
    assert_eq!(
        conformance_service_engine::sample::outbox::row_status(db.app_pool(), staged).await,
        "PUBLISHED"
    );

    conformance_service_engine::sample::outbox::rewind_to_pending(db.app_pool(), staged).await;
    assert_eq!(engine.beat(db.app_pool()).await.rows, 1);
    let pass = relay
        .last_pass()
        .expect("the hosted relay recorded its pass");
    assert_eq!(
        pass.duplicates, 1,
        "the republish carried the same dedup id and the broker answered duplicate"
    );

    assert_eq!(
        delivered_event_ids(&fabric, "se-observer-crash").await,
        vec![staged],
        "at-least-once on the wire, delivered once inside the duplicate window"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s15_a_failing_relay_backs_off_without_delaying_the_others() {
    let db = TestDb::fresh().await;
    seed_rows(db.app_pool(), 10).await;

    let failing = Arc::new(FailingSampleRelay::new(RelayName::from_static("broken")));
    let mut engine = runtime("se-relay-0").with_batch(2);
    engine
        .register_erased(failing.clone())
        .expect("the failing relay registers");
    engine
        .register(RowClaimSampleRelay::new(
            RelayName::from_static("sample_rows"),
            Duration::ZERO,
        ))
        .expect("the healthy relay registers");

    const BEATS: usize = 8;
    for _ in 0..BEATS {
        let round = engine.beat(db.app_pool()).await;
        assert!(round.ran >= 1, "the healthy relay ran on every beat");
    }

    assert!(
        failing.attempts() < BEATS,
        "the platform backoff holds a failing relay back instead of retrying it every beat, \
         but it tried {} times across {BEATS} beats",
        failing.attempts()
    );
    let unclaimed: i64 = scalar(
        db.app_pool(),
        "SELECT count(*) FROM sample_relay_row WHERE claimed_at IS NULL",
    )
    .await;
    assert_eq!(
        unclaimed, 0,
        "the failing relay never delayed the other one"
    );

    let board = engine.health().borrow().clone();
    assert!(!board.is_healthy());
    assert_eq!(
        board.degraded().collect::<Vec<_>>(),
        vec![&RelayName::from_static("broken")]
    );
    assert_eq!(
        board.condition(&RelayName::from_static("sample_rows")),
        Some(&RelayCondition::Healthy)
    );
    let broken = board
        .condition(&RelayName::from_static("broken"))
        .expect("the failing relay is on the board");
    assert!(
        matches!(broken, RelayCondition::BackingOff { retry_in, attempts, .. }
            if !retry_in.is_zero() && *attempts >= 1)
    );
    let reason = broken.reason().expect("a degraded relay names why");
    assert!(
        reason.contains("never publishes anything"),
        "readiness must be able to say why a relay is down, not only that it is: {reason}"
    );

    db.cleanup().await;
}

fn runtime(pod: &str) -> RelayRuntime {
    RelayRuntime::new(PodId::new(pod).expect("a valid pod id"))
        .with_batch(4)
        .with_lease(Duration::from_secs(30))
}

async fn beat_for(engine: &mut RelayRuntime, pg: &PgPool, rounds: usize, gap: Duration) {
    for _ in 0..rounds {
        engine.beat(pg).await;
        tokio::time::sleep(gap).await;
    }
}

async fn seed_rows(pool: &PgPool, count: usize) {
    for _ in 0..count {
        sqlx::query("INSERT INTO sample_relay_row (id) VALUES ($1)")
            .bind(Uuid::now_v7())
            .execute(pool)
            .await
            .expect("seed a claimable row");
    }
}

async fn scalar(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}
