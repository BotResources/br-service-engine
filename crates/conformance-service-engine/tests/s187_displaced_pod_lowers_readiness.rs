use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::SamplePrincipal;
use conformance_service_engine::sample::assignment::{Assignment, AssignmentProjector};
use conformance_service_engine::sample::engine_config;
use conformance_service_engine::sample::principal::{SamplePrincipalResolver, SampleRls};
use conformance_service_engine::sample::render::{assignment, attach_request, member, window};
use futures_util::StreamExt;
use service_engine::error::EngineError;
use service_engine::housekeeping::ready::REASON_SCHEMA_VERSION_DISPLACED;
use service_engine::{Engine, Readiness, ReadinessHandle};
use sqlx::PgPool;
use uuid::Uuid;

const STALLED_BEAT: Duration = Duration::from_secs(2);
const NEXT_LIVENESS: Duration = Duration::from_millis(500);
const NEXT_BEAT: Duration = Duration::from_millis(100);
const READY_WITHIN: Duration = Duration::from_secs(20);
const DOWN_WITHIN: Duration = Duration::from_secs(10);
const STOPPED_WITHIN: Duration = Duration::from_secs(20);
const STREAM_END_WITHIN: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn s187_a_pod_that_lost_the_schema_version_claim_stops_all_its_workers_and_run_names_the_loss()
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
    engine.bind_noun::<Assignment>().expect("bind the noun");
    engine.register_rls(SampleRls).expect("register the rls");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(AssignmentProjector)
        .expect("register the projector");
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    assignment(&pool, home, "alpha").await;
    let render = engine.render();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;
    let mut stream = render
        .attach(attach_request(
            &principal,
            vec![window(AssignmentProjector::NAME, false)],
        ))
        .await
        .expect("a session attaches while the pod owns the claim");

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

    let outcome = tokio::time::timeout(STOPPED_WITHIN, running)
        .await
        .expect("run returns once the heartbeat shows the claim was lost")
        .expect("the pod task joins");
    assert!(
        matches!(
            &outcome,
            Err(EngineError::SchemaClaimLost { service_version, .. }) if service_version == "1.0.0"
        ),
        "the displaced pod's run returns the lost claim, never Ok, got {outcome:?}",
    );
    assert_eq!(
        await_not_ready(&readiness).await,
        REASON_SCHEMA_VERSION_DISPLACED,
        "the displaced pod is DOWN with the displacement as its reason",
    );
    loop {
        match tokio::time::timeout(STREAM_END_WITHIN, stream.next()).await {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(_) => panic!(
                "the pod lost the claim but a live session's stream never ended, so its workers \
                 keep serving over a store another version owns"
            ),
        }
    }

    drop(next);
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
