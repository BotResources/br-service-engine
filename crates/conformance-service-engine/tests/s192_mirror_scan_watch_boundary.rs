//! The boundary is captured before the scan and the watch resumes at S + 1, so
//! a write that lands while the scan runs is either scanned or replayed — never
//! lost, and never mistaken for an orphan by the reconcile that follows.
mod mirror_support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::RecordingTransport;
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use mirror_support::{
    CATALOG_PREFIX, OBSERVED_WITHIN, PUBLISHED_LANGUAGE, await_count, await_watermark, catalog,
    email_of, last_sequence, persisted_keys, publish, retract, watermark,
};
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::mirror::{Project, Projection};
use service_engine::name::MirrorName;
use service_engine::nats::KvKey;
use service_engine::{Consumed, Mirror};
use uuid::Uuid;

const POLL: Duration = Duration::from_millis(20);

#[tokio::test]
async fn s192_the_watch_resumes_at_the_boundary_the_read_reached_and_replays_no_old_delete() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    // History the bucket still carries: a put and its retract, both before boot.
    let gone = Uuid::now_v7();
    publish(&fabric, "catalog/old", gone, "old").await;
    retract(&fabric, "catalog/old").await;
    let boundary = last_sequence(&fabric, PUBLISHED_LANGUAGE).await;

    let mirror = catalog("boundary_catalog").build(
        fabric.clone(),
        pool.clone(),
        Arc::new(RecordingTransport),
    );
    mirror
        .reconcile()
        .await
        .expect("a bucket whose only keys were retracted reads empty, and empty converges");
    assert_eq!(
        watermark(&pool, "boundary_catalog", PUBLISHED_LANGUAGE)
            .await
            .expect("the read committed its boundary")
            .0,
        boundary as i64,
        "the committed boundary is the stream's last sequence at the instant the scan started"
    );

    let watching = tokio::spawn(mirror.watch());
    await_deliver_policy(&fabric, boundary + 1).await;

    let live = Uuid::now_v7();
    publish(&fabric, "catalog/new", live, "new").await;
    await_count(&pool, 1, "the watch delivers what lands after the boundary").await;
    assert_eq!(email_of(&pool, live).await.as_deref(), Some("new"));

    watching.abort();
    drop(nats);
    db.cleanup().await;
}

/// The real JetStream consumer's delivery policy is the observable; a projector
/// call count would only say that something arrived, not from where.
async fn await_deliver_policy(fabric: &service_engine::nats::Nats, start_sequence: u64) {
    let want = async_nats::jetstream::consumer::DeliverPolicy::ByStartSequence { start_sequence };
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        let stream = fabric
            .bind_stream(&format!("KV_{PUBLISHED_LANGUAGE}"))
            .await
            .expect("the consumed bucket's stream");
        let consumers: Vec<_> = stream.consumers().collect().await;
        if consumers.iter().any(|consumer| {
            consumer
                .as_ref()
                .is_ok_and(|consumer| consumer.config.deliver_policy == want)
        }) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the watch must start at the boundary the read reached plus one, not at sequence 1 \
             and not at now"
        );
        tokio::time::sleep(POLL).await;
    }
}

// A slow consumer *decoder* holds the real scan open at a real read. There is
// no engine test hook, no fake broker, and no projector-call-count contract.
#[derive(Clone, Serialize)]
struct ScannedEntry {
    id: Uuid,
    value: String,
}

type ScanGate = (
    tokio::sync::Notify,
    std::sync::Mutex<bool>,
    std::sync::Condvar,
);
static SCAN: std::sync::OnceLock<ScanGate> = std::sync::OnceLock::new();
static PAUSED: AtomicBool = AtomicBool::new(false);

impl<'de> Deserialize<'de> for ScannedEntry {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let row = mirror_support::CatalogEntry::deserialize(deserializer)?;
        if row.value == "gate" && !PAUSED.swap(true, Ordering::SeqCst) {
            let (notify, lock, released) = SCAN.get().expect("the scan fixture is initialized");
            notify.notify_one();
            let held = lock.lock().unwrap();
            let (_guard, outcome) = released
                .wait_timeout_while(held, Duration::from_secs(10), |released| !*released)
                .unwrap();
            assert!(!outcome.timed_out(), "the writer must release the scan");
        }
        Ok(Self {
            id: row.id,
            value: row.value,
        })
    }
}

impl Consumed for ScannedEntry {
    const PREFIX: &'static str = CATALOG_PREFIX;
}

struct ScanProjection;

impl Project<Uuid> for ScanProjection {
    type Error = EngineError;

    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        id: Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let value = cx
                .shadow::<ScannedEntry>()
                .values()
                .find(|entry| entry.id == id)
                .map(|entry| entry.value.clone());
            match value {
                Some(value) => {
                    sqlx::query(
                        "INSERT INTO known_users (user_id, email) VALUES ($1, $2) \
                         ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email",
                    )
                    .bind(id)
                    .bind(value)
                    .execute(cx.conn())
                    .await?;
                }
                None => {
                    sqlx::query("DELETE FROM known_users WHERE user_id = $1")
                        .bind(id)
                        .execute(cx.conn())
                        .await?;
                }
            }
            Ok(())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s192_writes_during_the_scan_are_neither_skipped_nor_mistaken_for_orphans() {
    SCAN.set((
        tokio::sync::Notify::new(),
        std::sync::Mutex::new(false),
        std::sync::Condvar::new(),
    ))
    .expect("the scan fixture initializes once");
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();
    let bucket = fabric
        .published_language::<ScannedEntry>()
        .await
        .expect("bind the published-language bucket");

    let gate = Uuid::now_v7();
    let scanned = Uuid::now_v7();
    let gap = Uuid::now_v7();
    for (key, id, value) in [
        ("catalog/gate", gate, "gate"),
        ("catalog/one", scanned, "old"),
    ] {
        bucket
            .put(
                &KvKey::new(key).unwrap(),
                &ScannedEntry {
                    id,
                    value: value.to_string(),
                },
            )
            .await
            .expect("a row the producer published before boot");
    }
    sqlx::query("INSERT INTO known_users (user_id, email) VALUES ($1, 'local old')")
        .bind(scanned)
        .execute(&pool)
        .await
        .expect("the local row the scan is about to refresh");
    let boundary = last_sequence(&fabric, PUBLISHED_LANGUAGE).await;

    let mirror = Mirror::new(MirrorName::from_static("scan_catalog"))
        .consume::<ScannedEntry>()
        .keyed_by(|shadows, _| {
            shadows
                .shadow::<ScannedEntry>()
                .values()
                .map(|entry| entry.id)
                .collect()
        })
        .project(ScanProjection)
        .reconcile_keys(persisted_keys)
        .build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));

    let boot = tokio::spawn(mirror.reconcile());
    tokio::time::timeout(OBSERVED_WITHIN, SCAN.get().unwrap().0.notified())
        .await
        .expect("the scan reaches the gated row");
    for (key, id, value) in [("catalog/gap", gap, "gap"), ("catalog/one", scanned, "new")] {
        bucket
            .put(
                &KvKey::new(key).unwrap(),
                &ScannedEntry {
                    id,
                    value: value.to_string(),
                },
            )
            .await
            .expect("a producer writes while the scan is still running");
    }
    let (_, lock, released) = SCAN.get().unwrap();
    *lock.lock().unwrap() = true;
    released.notify_all();
    boot.await.unwrap().expect("the boot read converges");

    assert_eq!(
        email_of(&pool, scanned).await.as_deref(),
        Some("new"),
        "a key returned by the scan above the boundary is applied, never treated as an orphan"
    );
    assert_eq!(
        watermark(&pool, "scan_catalog", PUBLISHED_LANGUAGE)
            .await
            .expect("the boot committed its boundary")
            .0,
        boundary as i64
    );

    let replayed = bucket
        .get_with_revision(&KvKey::new("catalog/one").unwrap())
        .await
        .expect("read back the value written during the scan")
        .expect("it is still there")
        .1
        .get();
    let watching = tokio::spawn(mirror.watch());
    await_watermark(
        &pool,
        "scan_catalog",
        PUBLISHED_LANGUAGE,
        replayed as i64,
        "the watch must replay from the boundary, committing the value written during the scan \
         and not only the key it had never seen",
    )
    .await;
    await_count(&pool, 3, "gate, gap and the rewritten key all land").await;
    assert_eq!(
        email_of(&pool, scanned).await.as_deref(),
        Some("new"),
        "replaying a current value over the scanned one converges; the overlap is idempotent"
    );

    watching.abort();
    drop(nats);
    db.cleanup().await;
}
