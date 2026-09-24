use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::SamplePrincipal;
use conformance_service_engine::sample::engine_config;
use conformance_service_engine::sample::principal::SamplePrincipalResolver;
use service_engine::housekeeping::ready::REASON_SCHEMA_VERSION_DISPLACED;
use service_engine::{Engine, Readiness, ReadinessHandle};
use sqlx::PgPool;

const STALLED_BEAT: Duration = Duration::from_secs(2);
const NEXT_LIVENESS: Duration = Duration::from_millis(500);
const NEXT_BEAT: Duration = Duration::from_millis(100);
const READY_WITHIN: Duration = Duration::from_secs(20);
const DOWN_WITHIN: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn s187_a_running_pod_whose_heartbeat_lapsed_is_displaced_by_the_next_version_and_goes_down()
{
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let readiness = ReadinessHandle::not_ready("boot");
    let mut engine = Engine::<SamplePrincipal>::boot(
        engine_config("se_s187_a", "pod-s187-a")
            .with_service_version("1.0.0")
            .with_beat(STALLED_BEAT),
        pool.clone(),
        nats.nats().await,
        readiness.clone(),
    )
    .await
    .expect("the engine boots and claims the schema-version singleton");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let next = Engine::<SamplePrincipal>::boot(
        engine_config("se_s187_b", "pod-s187-b")
            .with_service_version("2.0.0")
            .with_beat(NEXT_BEAT)
            .with_schema_version_liveness(NEXT_LIVENESS),
        pool.clone(),
        nats.nats().await,
        ReadinessHandle::not_ready("boot"),
    )
    .await
    .expect(
        "the next version claims the singleton once the running pod's heartbeat is older than \
         its liveness, as a stalled beat leaves it",
    );
    assert_eq!(
        owner(&pool).await,
        ("2.0.0".to_string(), "pod-s187-b".to_string()),
        "the next version owns the schema version singleton",
    );

    let reason = await_not_ready(&readiness).await;
    assert_eq!(
        reason, REASON_SCHEMA_VERSION_DISPLACED,
        "the running pod's next heartbeat updates no row, so it recognises it was displaced and \
         goes DOWN rather than keep serving over a store another version owns",
    );

    drop(next);
    shutdown.notify_one();
    running
        .await
        .expect("the pod task joins")
        .expect("the pod run returns Ok");
    drop(nats);
    db.cleanup().await;
}

async fn owner(pool: &PgPool) -> (String, String) {
    sqlx::query_as("SELECT service_version, pod FROM service_engine.schema_version WHERE singleton")
        .fetch_one(pool)
        .await
        .expect("read the schema version singleton")
}

async fn await_ready(readiness: &ReadinessHandle) {
    let deadline = tokio::time::Instant::now() + READY_WITHIN;
    loop {
        if readiness.snapshot() == Readiness::Ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "readiness never rose to UP after a clean boot"
        );
        tokio::time::sleep(POLL).await;
    }
}

async fn await_not_ready(readiness: &ReadinessHandle) -> String {
    let deadline = tokio::time::Instant::now() + DOWN_WITHIN;
    loop {
        if let Readiness::NotReady { reason } = readiness.snapshot() {
            return reason;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "readiness stayed UP after the pod was displaced from the schema-version singleton"
        );
        tokio::time::sleep(POLL).await;
    }
}
