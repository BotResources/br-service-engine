use std::sync::Arc;
use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::SamplePrincipal;
use conformance_service_engine::sample::engine_config;
use conformance_service_engine::sample::principal::SamplePrincipalResolver;
use service_engine::config::EngineConfig;
use service_engine::error::EngineError;
use service_engine::nats::Nats;
use service_engine::{Engine, Readiness, ReadinessHandle};
use sqlx::PgPool;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;

const LIVENESS: Duration = Duration::from_secs(3);
const BEAT: Duration = Duration::from_millis(200);
const READY_WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(20);

struct RunningPod {
    shutdown: Arc<Notify>,
    task: JoinHandle<Result<(), EngineError>>,
}

impl RunningPod {
    async fn stop(self) {
        self.shutdown.notify_one();
        self.task
            .await
            .expect("the pod task joins")
            .expect("the pod stops gracefully");
    }
}

#[tokio::test]
async fn s136_a_second_version_is_refused_by_name_while_the_live_version_keeps_its_heartbeat_moving()
 {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let ready_a = ReadinessHandle::not_ready("boot");
    let pod_a = start_pod(
        engine_config("se_schemaver_live_a", "pod-a").with_service_version("1.0.0"),
        &pool,
        nats.nats().await,
        &ready_a,
    )
    .await;
    await_ready(&ready_a).await;

    let ready_b = ReadinessHandle::not_ready("boot");
    let nats_b = nats.nats().await;
    let started = Instant::now();
    let (boot_b, (wait_reason, reported_after)) = tokio::join!(
        Engine::<SamplePrincipal>::boot(
            next_version("se_schemaver_live_b"),
            pool.clone(),
            nats_b,
            ready_b.clone(),
        ),
        reason_naming_both_versions(&ready_b, started),
    );
    let waited = started.elapsed();

    assert!(
        reported_after < LIVENESS,
        "while it waits, the pod reports the handover with both versions named, long before it \
         could refuse: {wait_reason}",
    );
    match boot_b {
        Err(EngineError::SchemaVersionConflict {
            live_service,
            service_version,
            ..
        }) => {
            assert_eq!(
                live_service, "1.0.0",
                "the refusal names the live pod's service version"
            );
            assert_eq!(
                service_version, "2.0.0",
                "the refusal names the refused pod's service version"
            );
        }
        other => panic!(
            "a second version must be refused while the live one keeps beating, got {other:?}"
        ),
    }
    assert!(
        waited >= LIVENESS + BEAT,
        "the refusal comes only once the whole handover wait has passed with the other heartbeat \
         still fresh, never on first sight: refused after {waited:?}",
    );
    match ready_b.snapshot() {
        Readiness::NotReady { reason } => assert!(
            reason.contains("1.0.0") && reason.contains("2.0.0"),
            "the refused pod's readiness reason names both service versions: {reason}",
        ),
        Readiness::Ready => panic!("a refused pod must stay DOWN"),
    }
    assert_eq!(
        ready_a.snapshot(),
        Readiness::Ready,
        "the live version keeps serving"
    );
    assert_eq!(
        owner(&pool).await,
        ("1.0.0".to_string(), "pod-a".to_string()),
        "the live version keeps the schema version singleton",
    );

    pod_a.stop().await;
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s136_the_next_version_started_right_after_a_graceful_stop_waits_out_the_old_heartbeat_and_goes_up()
 {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let ready_a = ReadinessHandle::not_ready("boot");
    let pod_a = start_pod(
        engine_config("se_schemaver_stop_a", "pod-a").with_service_version("1.0.0"),
        &pool,
        nats.nats().await,
        &ready_a,
    )
    .await;
    await_ready(&ready_a).await;
    pod_a.stop().await;

    let still_fresh: bool = sqlx::query_scalar(
        "SELECT heartbeat > now() - make_interval(secs => $1) \
         FROM service_engine.schema_version WHERE singleton",
    )
    .bind(LIVENESS.as_secs_f64())
    .fetch_one(&pool)
    .await
    .expect("read the stopped version's heartbeat");
    assert!(
        still_fresh,
        "the stopped version's heartbeat is still fresh when the next version starts, so a pod \
         that refused on first sight would exit here",
    );

    let ready_b = ReadinessHandle::not_ready("boot");
    let pod_b = start_pod(
        next_version("se_schemaver_stop_b"),
        &pool,
        nats.nats().await,
        &ready_b,
    )
    .await;
    await_ready(&ready_b).await;
    assert_eq!(
        owner(&pool).await,
        ("2.0.0".to_string(), "pod-b".to_string()),
        "the next version claimed the schema version singleton once the old heartbeat lapsed",
    );

    pod_b.stop().await;
    drop(nats);
    db.cleanup().await;
}

fn next_version(channel: &str) -> EngineConfig {
    engine_config(channel, "pod-b")
        .with_service_version("2.0.0")
        .with_beat(BEAT)
        .with_schema_version_liveness(LIVENESS)
}

async fn start_pod(
    config: EngineConfig,
    pool: &PgPool,
    nats: Nats,
    readiness: &ReadinessHandle,
) -> RunningPod {
    let mut engine = Engine::<SamplePrincipal>::boot(config, pool.clone(), nats, readiness.clone())
        .await
        .expect("the pod boots and claims the schema version singleton");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    RunningPod {
        shutdown: engine.shutdown_handle(),
        task: tokio::spawn(engine.run()),
    }
}

async fn owner(pool: &PgPool) -> (String, String) {
    sqlx::query_as("SELECT service_version, pod FROM service_engine.schema_version WHERE singleton")
        .fetch_one(pool)
        .await
        .expect("read the schema version singleton")
}

async fn await_ready(readiness: &ReadinessHandle) {
    let deadline = Instant::now() + READY_WITHIN;
    while readiness.snapshot() != Readiness::Ready {
        assert!(Instant::now() < deadline, "readiness never rose to UP");
        tokio::time::sleep(POLL).await;
    }
}

async fn reason_naming_both_versions(
    readiness: &ReadinessHandle,
    started: Instant,
) -> (String, Duration) {
    let deadline = started + READY_WITHIN;
    loop {
        if let Readiness::NotReady { reason } = readiness.snapshot()
            && reason.contains("1.0.0")
            && reason.contains("2.0.0")
        {
            return (reason, started.elapsed());
        }
        assert!(
            Instant::now() < deadline,
            "no readiness reason ever named both versions"
        );
        tokio::time::sleep(POLL).await;
    }
}
