use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::{SampleKvSource, SampleRoster, StagingTransport};
use futures_util::future::BoxFuture;
use service_engine::error::RelayError;
use service_engine::housekeeping::relay::RelayRuntime;
use service_engine::impact::Impact;
use service_engine::inbound::{DEAD_LETTER_NOUN, DeadLetters};
use service_engine::name::{PodId, RelayName};
use service_engine::nats::KvKey;
use service_engine::relays::kv::{KvChange, KvDrainRelay, KvSource};
use service_engine::transport::ImpactTransport;
use sqlx::{PgConnection, PgPool};

const SLOT: Duration = Duration::from_millis(100);
const ROSTER_KEY: &str = "sample/rosters/one";
const CONFIDENTIAL: &str = "confidential-token-4417";

struct FailsAfterPublishing(SampleKvSource);

impl KvSource<SampleRoster> for FailsAfterPublishing {
    fn pending<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        batch: usize,
    ) -> BoxFuture<'a, Result<Vec<KvChange<SampleRoster>>, RelayError>> {
        self.0.pending(conn, batch)
    }

    fn applied<'a>(
        &'a self,
        _conn: &'a mut PgConnection,
        _applied: &'a [KvChange<SampleRoster>],
    ) -> BoxFuture<'a, Result<(), RelayError>> {
        Box::pin(async {
            Err(RelayError::Publish(
                "the sample source fails after the key is published".to_string(),
            ))
        })
    }
}

#[tokio::test]
async fn s282_the_kv_relay_dead_letters_an_undecodable_value_with_an_ops_impact() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let reader = fabric
        .published_language::<SampleRoster>()
        .await
        .expect("the fixed bucket binds");
    let key = KvKey::new(ROSTER_KEY).expect("a valid published-language key");
    let transport = StagingTransport::silent();
    let dead_letters = DeadLetters::new(db.app_pool().clone())
        .with_transport(transport.clone() as std::sync::Arc<dyn ImpactTransport>);

    let mut engine = runtime("se-leader-0");
    engine
        .register(
            KvDrainRelay::open(
                RelayName::from_static("kv_drain"),
                &fabric,
                SampleKvSource::new(Duration::ZERO),
            )
            .await
            .expect("the relay binds the published-language bucket")
            .with_dead_letters(dead_letters),
        )
        .expect("the kv relay registers");

    nats.put_raw(ROSTER_KEY, format!("not json {CONFIDENTIAL}").as_bytes())
        .await;
    queue(db.app_pool(), 1, Some("first")).await;
    tokio::time::sleep(SLOT).await;
    assert_eq!(engine.beat(db.app_pool()).await.rows, 1);

    assert_eq!(
        reader.get(&key).await.expect("read back"),
        Some(SampleRoster {
            label: "first".into(),
            version: 1
        }),
        "the bad value is overwritten by the authoritative state"
    );
    let rows = dead_letter_rows(db.app_pool()).await;
    assert_eq!(rows.len(), 1, "exactly one dead letter for the bad key");
    assert_eq!(rows[0].0, ROSTER_KEY);
    assert!(
        rows[0].1.starts_with("undecodable_value") && !rows[0].1.contains(CONFIDENTIAL),
        "the reason carries the stable code and never the value, got {:?}",
        rows[0].1
    );
    assert!(
        transport.staged().iter().any(|impact| matches!(
            impact,
            Impact::ResourceChanged { noun, .. } if *noun == DEAD_LETTER_NOUN
        )),
        "the dead letter stages an impact on the ops view"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s282_the_dead_letter_survives_a_drain_that_rolls_back_after_the_key_is_healed() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let reader = fabric
        .published_language::<SampleRoster>()
        .await
        .expect("the fixed bucket binds");
    let key = KvKey::new(ROSTER_KEY).expect("a valid published-language key");
    let dead_letters = DeadLetters::new(db.app_pool().clone());

    let mut engine = runtime("se-leader-0");
    engine
        .register(
            KvDrainRelay::open(
                RelayName::from_static("kv_drain"),
                &fabric,
                FailsAfterPublishing(SampleKvSource::new(Duration::ZERO)),
            )
            .await
            .expect("the relay binds the published-language bucket")
            .with_dead_letters(dead_letters),
        )
        .expect("the kv relay registers");

    nats.put_raw(ROSTER_KEY, b"not json").await;
    queue(db.app_pool(), 1, Some("first")).await;
    tokio::time::sleep(SLOT).await;
    let _ = engine.beat(db.app_pool()).await;

    assert_eq!(
        reader.get(&key).await.expect("read back"),
        Some(SampleRoster {
            label: "first".into(),
            version: 1
        }),
        "the key was healed in the bucket before the drain transaction rolled back"
    );
    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sample_kv_pending WHERE applied_at IS NULL")
            .fetch_one(db.app_pool())
            .await
            .expect("count pending changes");
    assert_eq!(
        pending, 1,
        "the drain rolled back, the change is not applied"
    );
    assert_eq!(
        dead_letter_rows(db.app_pool()).await.len(),
        1,
        "the only trace of the bad value is durable even though the drain rolled back"
    );

    db.cleanup().await;
}

fn runtime(pod: &str) -> RelayRuntime {
    RelayRuntime::new(PodId::new(pod).expect("a valid pod id"))
        .with_batch(8)
        .with_slot_period(SLOT)
        .with_lease(Duration::from_secs(30))
}

async fn dead_letter_rows(pool: &PgPool) -> Vec<(String, String)> {
    sqlx::query_as(
        "SELECT subject, error FROM service_engine.dead_letter \
         WHERE source = 'mirror' AND reaction = 'kv_drain'",
    )
    .fetch_all(pool)
    .await
    .expect("read the relay dead letters")
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
