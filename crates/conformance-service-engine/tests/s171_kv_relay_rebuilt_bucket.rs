use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::{SampleKvSource, SampleRoster};
use service_engine::housekeeping::relay::RelayRuntime;
use service_engine::name::{PodId, RelayName};
use service_engine::nats::KvKey;
use service_engine::relays::kv::KvDrainRelay;
use sqlx::PgPool;

const SLOT: Duration = Duration::from_millis(100);
const ROSTER_KEY: &str = "sample/rosters/one";

#[tokio::test]
async fn s171_reset_watermarks_repairs_a_truncated_and_rebuilt_bucket() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let bucket = fabric
        .published_language::<SampleRoster>()
        .await
        .expect("the fixed bucket binds");
    let key = KvKey::new(ROSTER_KEY).expect("a valid published-language key");

    let mut engine = RelayRuntime::new(PodId::new("se-leader-0").expect("a valid pod id"))
        .with_batch(8)
        .with_slot_period(SLOT)
        .with_lease(Duration::from_secs(30));
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
    drain(&mut engine, db.app_pool()).await;
    assert_eq!(
        bucket
            .get(&key)
            .await
            .expect("read back")
            .map(|r| r.version),
        Some(5),
        "the seed is published under leadership"
    );

    bucket
        .retract(&key)
        .await
        .expect("an operator truncates the published-language bucket out of band");

    queue(db.app_pool(), 5, Some("seed")).await;
    drain(&mut engine, db.app_pool()).await;
    assert_eq!(
        bucket.get(&key).await.expect("read back"),
        None,
        "the version guard refuses to re-put a key at its already-published version, so a bucket \
         an operator truncated and rebuilt stays empty for the unchanged keys — the drift the \
         reconcile of a value-changing offer cannot reach"
    );

    let repair = KvDrainRelay::open(
        RelayName::from_static("kv_drain"),
        &fabric,
        SampleKvSource::new(Duration::ZERO),
    )
    .await
    .expect("open a handle on the same relay to reset its watermarks");
    let mut conn = db.app_pool().acquire().await.expect("a pool connection");
    repair
        .reset_watermarks(&mut conn)
        .await
        .expect("reset the relay's per-key watermarks");
    drop(conn);

    queue(db.app_pool(), 5, Some("seed")).await;
    drain(&mut engine, db.app_pool()).await;
    assert_eq!(
        bucket.get(&key).await.expect("read back"),
        Some(SampleRoster {
            label: "seed".into(),
            version: 5
        }),
        "after reset_watermarks the source re-stages its set and the relay re-puts the unchanged \
         key, so the rebuilt bucket converges back to the store"
    );

    db.cleanup().await;
}

async fn drain(engine: &mut RelayRuntime, pg: &PgPool) {
    tokio::time::sleep(SLOT).await;
    engine.beat(pg).await;
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
