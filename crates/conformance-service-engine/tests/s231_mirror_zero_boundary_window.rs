mod mirror_support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::RecordingTransport;
use futures_util::future::BoxFuture;
use mirror_support::{
    CATALOG_PREFIX, OBSERVED_WITHIN, PUBLISHED_LANGUAGE, await_count, count, email_of,
    last_sequence, persisted_keys, watermark,
};
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::mirror::{Project, Projection};
use service_engine::name::MirrorName;
use service_engine::nats::KvKey;
use service_engine::{Consumed, Mirror};
use uuid::Uuid;

const NO_RECONCILE: Duration = Duration::from_secs(600);

type OpenGate = (
    tokio::sync::Notify,
    std::sync::Mutex<bool>,
    std::sync::Condvar,
);
static OPENING: std::sync::OnceLock<OpenGate> = std::sync::OnceLock::new();
static ARMED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Serialize, Deserialize)]
struct GatedEntry {
    id: Uuid,
    value: String,
}

impl Consumed for GatedEntry {
    const PREFIX: &'static str = CATALOG_PREFIX;

    fn bucket() -> &'static str {
        if ARMED.swap(false, Ordering::SeqCst) {
            let (notify, lock, released) = OPENING.get().expect("the gate is initialized");
            notify.notify_one();
            let held = lock.lock().unwrap();
            let (_guard, outcome) = released
                .wait_timeout_while(held, Duration::from_secs(10), |released| !*released)
                .unwrap();
            assert!(
                !outcome.timed_out(),
                "the writer must release the watch open"
            );
        }
        PUBLISHED_LANGUAGE
    }
}

struct GatedProjection;

impl Project<Uuid> for GatedProjection {
    type Error = EngineError;

    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        id: Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let value = cx
                .shadow::<GatedEntry>()
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
async fn s231_a_put_inside_the_zero_boundary_window_reaches_known_without_the_reconcile() {
    OPENING
        .set((
            tokio::sync::Notify::new(),
            std::sync::Mutex::new(false),
            std::sync::Condvar::new(),
        ))
        .expect("the gate initializes once");
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let mirror = Mirror::new(MirrorName::from_static("zero_window_catalog"))
        .consume::<GatedEntry>()
        .keyed_by(|shadows, _| {
            shadows
                .shadow::<GatedEntry>()
                .values()
                .map(|entry| entry.id)
                .collect()
        })
        .project(GatedProjection)
        .reconcile_keys(persisted_keys)
        .with_reconcile_deadline(NO_RECONCILE)
        .build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));

    mirror
        .reconcile()
        .await
        .expect("an empty bucket is a converged bucket with nothing in it");
    assert_eq!(
        watermark(&pool, "zero_window_catalog", PUBLISHED_LANGUAGE)
            .await
            .expect("the read committed its boundary")
            .0,
        0,
        "the first boot of every consumer resumes from zero"
    );
    assert_eq!(count(&pool).await, 0);

    ARMED.store(true, Ordering::SeqCst);
    let watching = tokio::spawn(mirror.watch());
    tokio::time::timeout(OBSERVED_WITHIN, OPENING.get().unwrap().0.notified())
        .await
        .expect("the watch open reaches the barrier, past the boundary it just read");

    let id = Uuid::now_v7();
    fabric
        .published_language::<GatedEntry>()
        .await
        .expect("bind the published-language bucket")
        .put(
            &KvKey::new("catalog/one").expect("a valid catalog key"),
            &GatedEntry {
                id,
                value: "published inside the window".to_string(),
            },
        )
        .await
        .expect("a producer publishes its first key");
    assert_eq!(last_sequence(&fabric, PUBLISHED_LANGUAGE).await, 1);

    let (_, lock, released) = OPENING.get().unwrap();
    *lock.lock().unwrap() = true;
    released.notify_all();

    await_count(
        &pool,
        1,
        "a put inside the zero-boundary window must reach known_* from the metadata read after \
         the subscription, not from the periodic reconcile",
    )
    .await;
    assert_eq!(
        email_of(&pool, id).await.as_deref(),
        Some("published inside the window")
    );

    watching.abort();
    drop(nats);
    db.cleanup().await;
}
