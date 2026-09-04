use std::time::Duration;

use br_util_nats_fabric::{KvKey, PublishedLanguageReader};
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::{SampleKvSource, SampleRoster};
use service_engine::housekeeping::relay::RelayRuntime;
use service_engine::name::{PodId, RelayName};
use service_engine::relays::kv::KvDrainRelay;
use sqlx::PgPool;

const SLOT: Duration = Duration::from_millis(100);
const ROSTER_KEY: &str = "sample/rosters/one";

#[tokio::test]
async fn s15_the_kv_relay_publishes_monotonically_under_leadership() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let reader = PublishedLanguageReader::<SampleRoster>::open(&fabric)
        .await
        .expect("the fixed bucket binds");
    let key = KvKey::new(ROSTER_KEY).expect("a valid published-language key");

    let mut engine = runtime("se-leader-0");
    engine
        .register(
            KvDrainRelay::open(
                RelayName::from_static("kv_drain"),
                &fabric,
                SampleKvSource::new(Duration::ZERO),
            )
            .await
            .expect("the relay binds the published-language bucket"),
        )
        .expect("the kv relay registers");

    queue(db.app_pool(), 1, Some("first")).await;
    assert_eq!(next_slot(&mut engine, db.app_pool()).await, 1);
    assert_eq!(
        reader.get(&key).await.expect("read back"),
        Some(SampleRoster {
            label: "first".into(),
            version: 1
        })
    );

    queue(db.app_pool(), 2, Some("second")).await;
    assert_eq!(next_slot(&mut engine, db.app_pool()).await, 1);
    assert_eq!(
        reader
            .get(&key)
            .await
            .expect("read back")
            .map(|r| r.version),
        Some(2)
    );

    queue(db.app_pool(), 1, Some("replayed")).await;
    assert_eq!(next_slot(&mut engine, db.app_pool()).await, 1);
    assert_eq!(
        reader.get(&key).await.expect("read back"),
        Some(SampleRoster {
            label: "second".into(),
            version: 2
        }),
        "a replayed older change never walks the published entry backwards"
    );

    queue(db.app_pool(), 3, None).await;
    assert_eq!(next_slot(&mut engine, db.app_pool()).await, 1);
    assert_eq!(
        reader.get(&key).await.expect("read back"),
        None,
        "a retraction deletes the entry under its observed revision"
    );

    let pending: i64 = scalar(
        db.app_pool(),
        "SELECT count(*) FROM sample_kv_pending WHERE applied_at IS NULL",
    )
    .await;
    assert_eq!(pending, 0, "every drained change is marked applied");

    db.cleanup().await;
}

#[tokio::test]
async fn s15_a_change_requeued_mid_drain_is_never_marked_applied_unpublished() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let reader = PublishedLanguageReader::<SampleRoster>::open(&fabric)
        .await
        .expect("the fixed bucket binds");
    let key = KvKey::new(ROSTER_KEY).expect("a valid published-language key");

    let mut engine = runtime("se-leader-0");
    engine
        .register(
            KvDrainRelay::open(
                RelayName::from_static("kv_drain"),
                &fabric,
                SampleKvSource::new(Duration::from_millis(300)),
            )
            .await
            .expect("the relay binds the published-language bucket"),
        )
        .expect("the kv relay registers");

    queue(db.app_pool(), 1, Some("first")).await;
    let writer = db
        .pool_as(db.app_role())
        .await
        .expect("a pool for the writer");

    tokio::time::sleep(SLOT).await;
    let requeue = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(60)).await;
        queue(&writer, 2, Some("second")).await;
        writer.close().await;
    });
    assert_eq!(engine.beat(db.app_pool()).await.rows, 1);
    requeue.await.expect("the concurrent writer finished");

    assert!(engine.health().borrow().is_healthy());
    assert_eq!(
        reader
            .get(&key)
            .await
            .expect("read back")
            .map(|r| r.version),
        Some(1),
        "the drain published the version it read"
    );
    let pending: i64 = scalar(
        db.app_pool(),
        "SELECT count(*) FROM sample_kv_pending WHERE applied_at IS NULL",
    )
    .await;
    assert_eq!(
        pending, 1,
        "a change re-queued while the relay was publishing must survive the applied stamp"
    );

    assert_eq!(next_slot(&mut engine, db.app_pool()).await, 1);
    assert_eq!(
        reader
            .get(&key)
            .await
            .expect("read back")
            .map(|r| r.version),
        Some(2),
        "the re-queued version reaches the bucket on the next drain"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s15_a_stale_put_after_a_newer_retract_never_resurrects_the_key() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let reader = PublishedLanguageReader::<SampleRoster>::open(&fabric)
        .await
        .expect("the fixed bucket binds");
    let key = KvKey::new(ROSTER_KEY).expect("a valid published-language key");

    let mut engine = runtime("se-leader-0");
    engine
        .register(
            KvDrainRelay::open(
                RelayName::from_static("kv_drain"),
                &fabric,
                SampleKvSource::new(Duration::ZERO),
            )
            .await
            .expect("the relay binds the published-language bucket"),
        )
        .expect("the kv relay registers");

    queue(db.app_pool(), 5, Some("seed")).await;
    assert_eq!(next_slot(&mut engine, db.app_pool()).await, 1);
    assert_eq!(
        reader
            .get(&key)
            .await
            .expect("read back")
            .map(|r| r.version),
        Some(5)
    );

    queue(db.app_pool(), 8, None).await;
    assert_eq!(next_slot(&mut engine, db.app_pool()).await, 1);
    assert_eq!(
        reader.get(&key).await.expect("read back"),
        None,
        "the retraction at version 8 deletes the entry"
    );

    queue(db.app_pool(), 7, Some("stale")).await;
    assert_eq!(next_slot(&mut engine, db.app_pool()).await, 1);
    assert_eq!(
        reader.get(&key).await.expect("read back"),
        None,
        "a Put at version 7, older than the retract at 8, must not resurrect the deleted key even \
         though the tombstone reads as an absent key"
    );

    queue(db.app_pool(), 9, Some("revived")).await;
    assert_eq!(next_slot(&mut engine, db.app_pool()).await, 1);
    assert_eq!(
        reader.get(&key).await.expect("read back"),
        Some(SampleRoster {
            label: "revived".into(),
            version: 9
        }),
        "a Put at version 9, newer than the retract at 8, writes"
    );

    let watermark: i64 = scalar(
        db.app_pool(),
        "SELECT version FROM service_engine.kv_relay_watermark WHERE relay = 'kv_drain'",
    )
    .await;
    assert_eq!(
        watermark, 9,
        "the per-key watermark that survives restarts records the newest version published"
    );

    let pending: i64 = scalar(
        db.app_pool(),
        "SELECT count(*) FROM sample_kv_pending WHERE applied_at IS NULL",
    )
    .await;
    assert_eq!(pending, 0, "every drained change is marked applied");

    db.cleanup().await;
}

fn runtime(pod: &str) -> RelayRuntime {
    RelayRuntime::new(PodId::new(pod).expect("a valid pod id"))
        .with_batch(8)
        .with_slot_period(SLOT)
        .with_lease(Duration::from_secs(30))
}

async fn next_slot(engine: &mut RelayRuntime, pg: &PgPool) -> usize {
    tokio::time::sleep(SLOT).await;
    engine.beat(pg).await.rows
}

async fn queue(pool: &PgPool, version: i64, label: Option<&str>) {
    sqlx::query(
        "INSERT INTO sample_kv_pending (key, version, label, applied_at) \
         VALUES ($1, $2, $3, NULL) \
         ON CONFLICT (key) DO UPDATE \
            SET version = EXCLUDED.version, label = EXCLUDED.label, applied_at = NULL",
    )
    .bind(ROSTER_KEY)
    .bind(version)
    .bind(label)
    .execute(pool)
    .await
    .expect("queue a published-language change");
}

async fn scalar(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}
