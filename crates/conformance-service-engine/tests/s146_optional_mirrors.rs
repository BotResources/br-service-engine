//! Generic optional catalogs use payload-derived projection keys, never a constant key.
use conformance_service_engine::{TestDb, infra::TestNats, sample::RecordingTransport};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::{
    Consumed, Mirror,
    error::EngineError,
    mirror::{Project, Projection},
    name::MirrorName,
    nats::KvKey,
};
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
struct CatalogEntry {
    id: Uuid,
    value: String,
}
impl Consumed for CatalogEntry {
    const PREFIX: &'static str = "catalog/";
    const ALLOW_EMPTY: bool = true;
}
struct CatalogProjection;
impl Project<Uuid> for CatalogProjection {
    type Error = EngineError;
    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        id: Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let value = cx
                .shadow::<CatalogEntry>()
                .values()
                .find(|v| v.id == id)
                .map(|v| v.value.clone());
            if let Some(value) = value {
                sqlx::query("INSERT INTO known_users(user_id,email) VALUES($1,$2) ON CONFLICT(user_id) DO UPDATE SET email=EXCLUDED.email").bind(id).bind(value).execute(cx.conn()).await?;
            } else {
                sqlx::query("DELETE FROM known_users WHERE user_id=$1")
                    .bind(id)
                    .execute(cx.conn())
                    .await?;
            }
            Ok(())
        })
    }
}
fn catalog() -> service_engine::mirror::MirrorReady<Uuid, CatalogProjection> {
    Mirror::new(MirrorName::from_static("optional_catalog"))
        .consume::<CatalogEntry>()
        .keyed_by(|shadows, _| {
            shadows
                .shadow::<CatalogEntry>()
                .values()
                .map(|v| v.id)
                .collect()
        })
        .project(CatalogProjection)
}
fn reconciled() -> service_engine::mirror::MirrorReady<Uuid, CatalogProjection> {
    catalog().reconcile_keys(|pool| {
        Box::pin(async move {
            Ok(sqlx::query_scalar("SELECT user_id FROM known_users")
                .fetch_all(&pool)
                .await?)
        })
    })
}
async fn count(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM known_users")
        .fetch_one(pool)
        .await
        .unwrap()
}
async fn await_count(pool: &sqlx::PgPool, expected: i64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while count(pool).await != expected {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("projection converges");
}
#[tokio::test]
async fn optional_catalog_requires_reconcile_keys_at_startup() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let error = catalog()
        .build(
            nats.nats().await,
            db.app_pool().clone(),
            Arc::new(RecordingTransport),
        )
        .reconcile()
        .await
        .expect_err("optional without reconciliation keys must fail");
    assert!(error.to_string().contains("reconcile_keys"), "{error}");
    db.cleanup().await;
}
#[tokio::test]
async fn optional_empty_boot_and_last_delete_reconcile_payload_keys() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool();
    sqlx::query("INSERT INTO known_users(user_id,email) VALUES($1,'stale')")
        .bind(Uuid::now_v7())
        .execute(pool)
        .await
        .unwrap();
    let fabric = nats.nats().await;
    let mirror = reconciled().build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    mirror.reconcile().await.unwrap();
    assert_eq!(count(pool).await, 0);
    let bucket = fabric.published_language::<CatalogEntry>().await.unwrap();
    // The UUID is only in the value; a delete cannot derive it from this key.
    let key = KvKey::new("catalog/arbitrary-source-key").unwrap();
    bucket
        .put(
            &key,
            &CatalogEntry {
                id: Uuid::now_v7(),
                value: "current".into(),
            },
        )
        .await
        .unwrap();
    // Deliberately publish in the S=0 snapshot/watch gap.
    let watching = tokio::spawn(mirror.watch());
    await_count(pool, 1).await;
    bucket.retract(&key).await.unwrap();
    await_count(pool, 0).await;
    watching.abort();
    db.cleanup().await;
}

#[derive(Clone, Serialize, Deserialize)]
struct Marker {
    value: String,
}
impl Consumed for Marker {
    const PREFIX: &'static str = "producer-ready/";
}
fn guarded() -> service_engine::mirror::MirrorReady<Uuid, CatalogProjection> {
    Mirror::new(MirrorName::from_static("guarded_catalog"))
        .consume::<CatalogEntry>()
        .consume::<Marker>()
        .keyed_by(|s, _| s.shadow::<CatalogEntry>().values().map(|v| v.id).collect())
        .project(CatalogProjection)
        .reconcile_keys(|pool| {
            Box::pin(async move {
                Ok(sqlx::query_scalar("SELECT user_id FROM known_users")
                    .fetch_all(&pool)
                    .await?)
            })
        })
}
async fn watermark(pool: &sqlx::PgPool, name: &str, bucket: &str) -> Option<(i64, Option<String>)> {
    sqlx::query_as("SELECT revision,stream_created_at FROM service_engine.mirror_watermark WHERE mirror=$1 AND bucket=$2")
        .bind(name).bind(bucket).fetch_optional(pool).await.unwrap()
}
async fn publish(fabric: &service_engine::nats::Nats, key: &str, id: Uuid, value: &str) {
    fabric
        .published_language::<CatalogEntry>()
        .await
        .unwrap()
        .put(
            &KvKey::new(key).unwrap(),
            &CatalogEntry {
                id,
                value: value.into(),
            },
        )
        .await
        .unwrap();
}
async fn ready(tasks: &mut service_engine::housekeeping::mirror::MirrorTasks) {
    assert!(
        tokio::time::timeout(Duration::from_secs(15), tasks.converged())
            .await
            .unwrap()
    );
}
async fn restarting(
    board: &service_engine::housekeeping::mirror::MirrorsHealthReceiver,
    name: &'static str,
) -> String {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(reason) = board
                .borrow()
                .condition(&MirrorName::from_static(name))
                .and_then(|c| c.reason())
                .map(str::to_owned)
            {
                return reason;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("supervisor reports the failed mirror")
}
#[tokio::test]
async fn required_marker_preserves_rows_and_recovers_in_the_same_supervisor() {
    use service_engine::housekeeping::{mirror::MirrorSupervisor, ready::ReadinessAssembly};
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool();
    let id = Uuid::now_v7();
    // Prior local state is not a reason to trust an absent required source.
    sqlx::query("INSERT INTO known_users(user_id,email) VALUES($1,'preserved')")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
    let mirror = guarded().build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    let mut supervisor = MirrorSupervisor::new();
    supervisor.register(mirror).unwrap();
    let board = supervisor.health();
    let readiness = ReadinessAssembly::new(service_engine::ReadinessHandle::ready(), board.clone());
    let mut tasks = supervisor.start(Arc::new(tokio::sync::Notify::new()));
    let reason = restarting(&board, "guarded_catalog").await;
    assert!(reason.contains("producer-ready/"));
    assert!(!readiness.refresh().is_ready());
    assert_eq!(count(pool).await, 1);
    assert!(
        watermark(pool, "guarded_catalog", "PUBLISHED_LANGUAGE")
            .await
            .is_none()
    );
    let markers = fabric.published_language::<Marker>().await.unwrap();
    let key = KvKey::new("producer-ready/marker").unwrap();
    markers
        .put(
            &key,
            &Marker {
                value: "arbitrary marker".into(),
            },
        )
        .await
        .unwrap();
    ready(&mut tasks).await;
    assert!(readiness.refresh().is_ready());
    assert_eq!(count(pool).await, 0);
    publish(&fabric, "catalog/present", id, "current").await;
    await_count(pool, 1).await;
    let before = watermark(pool, "guarded_catalog", "PUBLISHED_LANGUAGE").await;
    markers.retract(&key).await.unwrap();
    restarting(&board, "guarded_catalog").await;
    assert!(!readiness.refresh().is_ready());
    assert_eq!(count(pool).await, 1);
    assert_eq!(
        watermark(pool, "guarded_catalog", "PUBLISHED_LANGUAGE").await,
        before
    );
    markers
        .put(
            &key,
            &Marker {
                value: "returned".into(),
            },
        )
        .await
        .unwrap();
    ready(&mut tasks).await;
    assert!(readiness.refresh().is_ready());
    assert_eq!(count(pool).await, 1);
    tasks.abort();
    db.cleanup().await;
}

#[tokio::test]
async fn history_greater_than_one_is_rejected_before_projection() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let stream = fabric
        .context()
        .get_stream("KV_PUBLISHED_LANGUAGE")
        .await
        .unwrap();
    let mut config = stream.get_info().await.unwrap().config;
    config.max_messages_per_subject = 2;
    fabric.context().update_stream(config).await.unwrap();
    let id = Uuid::now_v7();
    sqlx::query("INSERT INTO known_users(user_id,email) VALUES($1,'preserved')")
        .bind(id)
        .execute(db.app_pool())
        .await
        .unwrap();
    let error = reconciled()
        .build(fabric, db.app_pool().clone(), Arc::new(RecordingTransport))
        .reconcile()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("history must be 1"), "{error}");
    assert_eq!(count(db.app_pool()).await, 1);
    assert!(
        watermark(db.app_pool(), "optional_catalog", "PUBLISHED_LANGUAGE")
            .await
            .is_none()
    );
    db.cleanup().await;
}

#[tokio::test]
async fn empty_prefix_uses_the_bucket_boundary_and_does_not_replay_historical_deletes() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let bucket = fabric.published_language::<CatalogEntry>().await.unwrap();
    let id = Uuid::now_v7();
    publish(&fabric, "catalog/old", id, "old").await;
    bucket
        .retract(&KvKey::new("catalog/old").unwrap())
        .await
        .unwrap();
    let s = fabric
        .context()
        .get_stream("KV_PUBLISHED_LANGUAGE")
        .await
        .unwrap()
        .get_info()
        .await
        .unwrap()
        .state
        .last_sequence;
    let mirror = reconciled().build(
        fabric.clone(),
        db.app_pool().clone(),
        Arc::new(RecordingTransport),
    );
    mirror.reconcile().await.unwrap();
    assert_eq!(
        watermark(db.app_pool(), "optional_catalog", "PUBLISHED_LANGUAGE")
            .await
            .unwrap()
            .0,
        s as i64
    );
    let watching = tokio::spawn(mirror.watch());
    // Inspect the real JetStream consumer's delivery policy, not projector count.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            use futures_util::StreamExt;
            let stream = fabric
                .context()
                .get_stream("KV_PUBLISHED_LANGUAGE")
                .await
                .unwrap();
            let consumers: Vec<_> = stream.consumers().collect().await;
            if consumers.iter().any(|c| {
                c.as_ref().is_ok_and(|c| {
                    c.config.deliver_policy
                        == async_nats::jetstream::consumer::DeliverPolicy::ByStartSequence {
                            start_sequence: s + 1,
                        }
                })
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("watch starts from bucket S + 1, not sequence 1");
    publish(&fabric, "catalog/new", id, "new").await;
    await_count(db.app_pool(), 1).await;
    watching.abort();
    db.cleanup().await;
}

#[tokio::test]
async fn empty_standby_waits_for_the_leaders_committed_bucket_snapshot() {
    use service_engine::{MirrorLeader, name::PodId};
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool();
    // An unrelated key establishes a nonzero bucket boundary despite empty catalog.
    fabric
        .published_language::<Marker>()
        .await
        .unwrap()
        .put(
            &KvKey::new("producer-ready/unrelated").unwrap(),
            &Marker {
                value: "present".into(),
            },
        )
        .await
        .unwrap();
    sqlx::query("INSERT INTO known_users(user_id,email) VALUES($1,'stale')")
        .bind(Uuid::now_v7())
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO service_engine.leader_slot(name,slot,pod,lease_until,completed_at) VALUES('mirror:optional_catalog','epoch'::timestamptz,'leader',now()+interval '1 hour',NULL)").execute(pool).await.unwrap();
    let leader = |name| {
        MirrorLeader::new(
            PodId::new(name).unwrap(),
            Duration::from_secs(10),
            Duration::from_millis(30),
        )
    };
    let standby = reconciled().build_led(
        fabric.clone(),
        pool.clone(),
        Arc::new(RecordingTransport),
        leader("standby"),
    );
    let waiting = tokio::spawn(standby.reconcile());
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(!waiting.is_finished());
    assert_eq!(count(pool).await, 1);
    reconciled()
        .build_led(
            fabric,
            pool.clone(),
            Arc::new(RecordingTransport),
            leader("leader"),
        )
        .reconcile()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(count(pool).await, 0);
    assert!(
        watermark(pool, "optional_catalog", "PUBLISHED_LANGUAGE")
            .await
            .unwrap()
            .1
            .is_some()
    );
    db.cleanup().await;
}

#[tokio::test]
async fn rollback_or_replacement_preserves_state_and_watermark_until_explicit_reset() {
    use service_engine::housekeeping::{mirror::MirrorSupervisor, ready::ReadinessAssembly};
    for replacement in [false, true] {
        let db = TestDb::fresh().await;
        let nats = TestNats::spawn().await;
        nats.provision().await;
        let fabric = nats.nats().await;
        let pool = db.app_pool();
        let id = Uuid::now_v7();
        publish(&fabric, "catalog/old", id, "old").await;
        reconciled()
            .build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport))
            .reconcile()
            .await
            .unwrap();
        if replacement {
            fabric
                .context()
                .delete_key_value("PUBLISHED_LANGUAGE")
                .await
                .unwrap();
            fabric
                .context()
                .create_key_value(async_nats::jetstream::kv::Config {
                    bucket: "PUBLISHED_LANGUAGE".into(),
                    history: 1,
                    ..Default::default()
                })
                .await
                .unwrap();
            // Its sequence overtakes the old watermark; only identity catches it.
            for n in 0..4 {
                publish(&fabric, "catalog/replacement", id, &format!("new-{n}")).await;
            }
        } else {
            // A persisted sequence ahead of the broker models restoring older
            // broker state with its original creation identity intact.
            sqlx::query("UPDATE service_engine.mirror_watermark SET revision=revision+100 WHERE mirror='optional_catalog'").execute(pool).await.unwrap();
        }
        let before = watermark(pool, "optional_catalog", "PUBLISHED_LANGUAGE").await;
        let mut supervisor = MirrorSupervisor::new();
        supervisor
            .register(reconciled().build(
                fabric.clone(),
                pool.clone(),
                Arc::new(RecordingTransport),
            ))
            .unwrap();
        let board = supervisor.health();
        let readiness =
            ReadinessAssembly::new(service_engine::ReadinessHandle::ready(), board.clone());
        let mut tasks = supervisor.start(Arc::new(tokio::sync::Notify::new()));
        let reason = restarting(&board, "optional_catalog").await;
        assert!(
            reason.contains(if replacement {
                "identity changed"
            } else {
                "sequence rollback"
            }),
            "{reason}"
        );
        let recovery = "DELETE FROM service_engine.mirror_watermark WHERE mirror = 'optional_catalog' AND bucket = 'PUBLISHED_LANGUAGE';";
        let service_engine::Readiness::NotReady { reason } = readiness.refresh() else {
            panic!("incompatible bucket must not be ready");
        };
        assert!(
            reason.contains(recovery),
            "readiness must expose exact recovery: {reason}"
        );
        assert_eq!(
            watermark(pool, "optional_catalog", "PUBLISHED_LANGUAGE").await,
            before
        );
        let value: String = sqlx::query_scalar("SELECT email FROM known_users WHERE user_id=$1")
            .bind(id)
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(value, "old");
        sqlx::query(recovery).execute(pool).await.unwrap();
        ready(&mut tasks).await;
        assert!(readiness.refresh().is_ready());
        let after = watermark(pool, "optional_catalog", "PUBLISHED_LANGUAGE")
            .await
            .unwrap();
        assert!(after.1.is_some());
        if replacement {
            assert_ne!(after.1, before.unwrap().1);
        } else {
            assert!(after.0 < before.unwrap().0);
        }
        tasks.abort();
        db.cleanup().await;
    }
}

// A slow *consumer decoder* coordinates the test at a real scan read. There is
// no engine test hook, fake broker, or synthetic projector-call-count contract.
#[derive(Clone, Serialize)]
struct ScannedEntry {
    id: Uuid,
    value: String,
}
static SCAN: std::sync::OnceLock<(
    tokio::sync::Notify,
    std::sync::Mutex<bool>,
    std::sync::Condvar,
)> = std::sync::OnceLock::new();
static PAUSED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
impl<'de> Deserialize<'de> for ScannedEntry {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let row = CatalogEntry::deserialize(d)?;
        if row.value == "gate" && !PAUSED.swap(true, std::sync::atomic::Ordering::SeqCst) {
            let (notify, lock, cond) = SCAN.get().expect("scan fixture initialized");
            notify.notify_one();
            let released = lock.lock().unwrap();
            let (_guard, outcome) = cond
                .wait_timeout_while(released, Duration::from_secs(10), |released| !*released)
                .unwrap();
            assert!(
                !outcome.timed_out(),
                "test writer must release the real scan"
            );
        }
        Ok(Self {
            id: row.id,
            value: row.value,
        })
    }
}
impl Consumed for ScannedEntry {
    const PREFIX: &'static str = "scan/";
    const ALLOW_EMPTY: bool = true;
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
                .find(|v| v.id == id)
                .map(|v| v.value.clone());
            if let Some(value) = value {
                sqlx::query("INSERT INTO known_users(user_id,email) VALUES($1,$2) ON CONFLICT(user_id) DO UPDATE SET email=EXCLUDED.email").bind(id).bind(value).execute(cx.conn()).await?;
            } else {
                sqlx::query("DELETE FROM known_users WHERE user_id=$1")
                    .bind(id)
                    .execute(cx.conn())
                    .await?;
            }
            Ok(())
        })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_during_scan_are_not_skipped_or_mistaken_for_orphans_and_replay_converges() {
    SCAN.set((
        tokio::sync::Notify::new(),
        std::sync::Mutex::new(false),
        std::sync::Condvar::new(),
    ))
    .unwrap();
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let bucket = fabric.published_language::<ScannedEntry>().await.unwrap();
    let gate = Uuid::now_v7();
    let scanned = Uuid::now_v7();
    let gap = Uuid::now_v7();
    for (key, id, value) in [
        ("scan/gate", gate, "gate"),
        ("scan/scanned", scanned, "old"),
    ] {
        bucket
            .put(
                &KvKey::new(key).unwrap(),
                &ScannedEntry {
                    id,
                    value: value.into(),
                },
            )
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO known_users(user_id,email) VALUES($1,'local old')")
        .bind(scanned)
        .execute(db.app_pool())
        .await
        .unwrap();
    let s = fabric
        .context()
        .get_stream("KV_PUBLISHED_LANGUAGE")
        .await
        .unwrap()
        .get_info()
        .await
        .unwrap()
        .state
        .last_sequence;
    let mirror = Mirror::new(MirrorName::from_static("scan_catalog"))
        .consume::<ScannedEntry>()
        .keyed_by(|s, _| s.shadow::<ScannedEntry>().values().map(|v| v.id).collect())
        .project(ScanProjection)
        .reconcile_keys(|pool| {
            Box::pin(async move {
                Ok(sqlx::query_scalar("SELECT user_id FROM known_users")
                    .fetch_all(&pool)
                    .await?)
            })
        })
        .build(
            fabric.clone(),
            db.app_pool().clone(),
            Arc::new(RecordingTransport),
        );
    let boot = tokio::spawn(mirror.reconcile());
    tokio::time::timeout(Duration::from_secs(5), SCAN.get().unwrap().0.notified())
        .await
        .unwrap();
    bucket
        .put(
            &KvKey::new("scan/gap").unwrap(),
            &ScannedEntry {
                id: gap,
                value: "gap".into(),
            },
        )
        .await
        .unwrap();
    bucket
        .put(
            &KvKey::new("scan/scanned").unwrap(),
            &ScannedEntry {
                id: scanned,
                value: "new".into(),
            },
        )
        .await
        .unwrap();
    let (_, lock, cond) = SCAN.get().unwrap();
    *lock.lock().unwrap() = true;
    cond.notify_all();
    boot.await.unwrap().unwrap();
    let value: String = sqlx::query_scalar("SELECT email FROM known_users WHERE user_id=$1")
        .bind(scanned)
        .fetch_one(db.app_pool())
        .await
        .expect("a key returned after S must not become an orphan");
    assert_eq!(value, "new");
    assert_eq!(
        watermark(db.app_pool(), "scan_catalog", "PUBLISHED_LANGUAGE")
            .await
            .unwrap()
            .0,
        s as i64
    );
    let replay_revision = bucket
        .get_with_revision(&KvKey::new("scan/scanned").unwrap())
        .await
        .unwrap()
        .unwrap()
        .1
        .get();
    let watching = tokio::spawn(mirror.watch());
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let held = watermark(db.app_pool(), "scan_catalog", "PUBLISHED_LANGUAGE")
                .await
                .unwrap();
            if held.0 >= replay_revision as i64 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the watch commits the replayed scanned value, not only the gap write");
    assert_eq!(count(db.app_pool()).await, 3);
    // Replaying the scanned value may project it again; only convergence matters.
    let value: String = sqlx::query_scalar("SELECT email FROM known_users WHERE user_id=$1")
        .bind(scanned)
        .fetch_one(db.app_pool())
        .await
        .unwrap();
    assert_eq!(value, "new");
    watching.abort();
    db.cleanup().await;
}

#[derive(Clone, Serialize, Deserialize)]
struct OtherEntry {
    id: Uuid,
    value: String,
}
impl Consumed for OtherEntry {
    const PREFIX: &'static str = "catalog/";
    const ALLOW_EMPTY: bool = true;
    fn bucket() -> &'static str {
        "OTHER_LANGUAGE"
    }
}
struct JoinedProjection;
impl Project<Uuid> for JoinedProjection {
    type Error = EngineError;
    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        id: Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let value = cx
                .shadow::<CatalogEntry>()
                .values()
                .find(|v| v.id == id)
                .map(|v| v.value.clone())
                .or_else(|| {
                    cx.shadow::<OtherEntry>()
                        .values()
                        .find(|v| v.id == id)
                        .map(|v| v.value.clone())
                });
            if let Some(value) = value {
                sqlx::query("INSERT INTO known_users(user_id,email) VALUES($1,$2) ON CONFLICT(user_id) DO UPDATE SET email=EXCLUDED.email").bind(id).bind(value).execute(cx.conn()).await?;
            } else {
                sqlx::query("DELETE FROM known_users WHERE user_id=$1")
                    .bind(id)
                    .execute(cx.conn())
                    .await?;
            }
            Ok(())
        })
    }
}
#[tokio::test]
async fn identities_and_boundaries_are_per_bucket_even_for_identical_prefixes() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    fabric
        .context()
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: "OTHER_LANGUAGE".into(),
            history: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    let first = Uuid::now_v7();
    publish(&fabric, "catalog/one", first, "one").await;
    let other = fabric
        .bind_kv::<OtherEntry>("OTHER_LANGUAGE")
        .await
        .unwrap();
    let second = Uuid::now_v7();
    let key = KvKey::new("catalog/two").unwrap();
    for n in 0..5 {
        other
            .put(
                &key,
                &OtherEntry {
                    id: second,
                    value: format!("two-{n}"),
                },
            )
            .await
            .unwrap();
    }
    let mirror = Mirror::new(MirrorName::from_static("joined"))
        .consume::<CatalogEntry>()
        .consume::<OtherEntry>()
        .keyed_by(|s, _| {
            s.shadow::<CatalogEntry>()
                .values()
                .map(|v| v.id)
                .chain(s.shadow::<OtherEntry>().values().map(|v| v.id))
                .collect()
        })
        .project(JoinedProjection)
        .reconcile_keys(|pool| {
            Box::pin(async move {
                Ok(sqlx::query_scalar("SELECT user_id FROM known_users")
                    .fetch_all(&pool)
                    .await?)
            })
        })
        .build(
            fabric.clone(),
            db.app_pool().clone(),
            Arc::new(RecordingTransport),
        );
    mirror.reconcile().await.unwrap();
    assert_eq!(count(db.app_pool()).await, 2);
    let one = watermark(db.app_pool(), "joined", "PUBLISHED_LANGUAGE")
        .await
        .unwrap();
    let two = watermark(db.app_pool(), "joined", "OTHER_LANGUAGE")
        .await
        .unwrap();
    assert_eq!(one.0, 1);
    assert_eq!(two.0, 5);
    assert_ne!(one.1, two.1);
    let watching = tokio::spawn(mirror.watch());
    other.retract(&key).await.unwrap();
    await_count(db.app_pool(), 1).await;
    assert_eq!(
        watermark(db.app_pool(), "joined", "PUBLISHED_LANGUAGE")
            .await
            .unwrap(),
        one
    );
    watching.abort();
    db.cleanup().await;
}
struct FailingProjection;
impl Project<Uuid> for FailingProjection {
    type Error = EngineError;
    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        id: Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("DELETE FROM known_users WHERE user_id=$1")
                .bind(id)
                .execute(cx.conn())
                .await?;
            Err(EngineError::Config("deliberate projector failure".into()))
        })
    }
}
#[tokio::test]
async fn failed_reconciliation_does_not_persist_identity_or_watermark() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    sqlx::query("INSERT INTO known_users(user_id,email) VALUES($1,'preserved')")
        .bind(Uuid::now_v7())
        .execute(db.app_pool())
        .await
        .unwrap();
    let mirror = Mirror::new(MirrorName::from_static("failed"))
        .consume::<CatalogEntry>()
        .keyed_by(|s, _| s.shadow::<CatalogEntry>().values().map(|v| v.id).collect())
        .project(FailingProjection)
        .reconcile_keys(|pool| {
            Box::pin(async move {
                Ok(sqlx::query_scalar("SELECT user_id FROM known_users")
                    .fetch_all(&pool)
                    .await?)
            })
        })
        .build(fabric, db.app_pool().clone(), Arc::new(RecordingTransport));
    assert!(mirror.reconcile().await.is_err());
    assert_eq!(count(db.app_pool()).await, 1);
    assert!(
        watermark(db.app_pool(), "failed", "PUBLISHED_LANGUAGE")
            .await
            .is_none()
    );
    db.cleanup().await;
}
#[tokio::test]
async fn first_adoption_trusts_the_current_bucket_even_with_legacy_rows_and_cursor() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    sqlx::query("INSERT INTO known_users(user_id,email) VALUES($1,'legacy')")
        .bind(Uuid::now_v7())
        .execute(db.app_pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO service_engine.mirror_watermark(mirror,bucket,revision) VALUES('optional_catalog','PUBLISHED_LANGUAGE',123)").execute(db.app_pool()).await.unwrap();
    reconciled()
        .build(
            nats.nats().await,
            db.app_pool().clone(),
            Arc::new(RecordingTransport),
        )
        .reconcile()
        .await
        .unwrap();
    let held = watermark(db.app_pool(), "optional_catalog", "PUBLISHED_LANGUAGE")
        .await
        .unwrap();
    assert_eq!(held.0, 0);
    assert!(held.1.is_some());
    assert_eq!(count(db.app_pool()).await, 0);
    db.cleanup().await;
}
