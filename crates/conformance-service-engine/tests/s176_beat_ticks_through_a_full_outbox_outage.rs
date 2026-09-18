use std::time::Duration;

use chrono::{DateTime, Utc};
use conformance_service_engine::infra::listener::engine_config;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::assignment::{Assignment, AssignmentProjector};
use conformance_service_engine::sample::principal::{SamplePrincipalResolver, SampleRls};
use conformance_service_engine::sample::{SampleCronJob, completed_slots, stage_outbox_row};
use service_engine::Engine;
use service_engine::cron::Schedule;
use service_engine::{Readiness, ReadinessHandle};
use sqlx::PgPool;

const BACKLOG: usize = 200;
const JOB: &str = "s176_beat";
const POD: &str = "pod-s176";
const NATS_GRACE: Duration = Duration::from_secs(2);
const READY_WITHIN: Duration = Duration::from_secs(25);
const OUTAGE_OBSERVE: Duration = Duration::from_secs(2);
const POLL: Duration = Duration::from_millis(100);

#[tokio::test]
async fn s176_a_full_outbox_never_freezes_the_beat_while_nats_is_down() {
    let db = TestDb::fresh().await;
    let mut nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let config = engine_config("se_s176_beat_ticks", POD)
        .with_beat(Duration::from_millis(100))
        .with_lease(Duration::from_secs(2))
        .with_nats_grace(NATS_GRACE);
    let readiness = ReadinessHandle::ready();
    let mut engine = Engine::boot(config, pool.clone(), fabric, readiness.clone())
        .await
        .expect("the engine boots while nats is reachable");
    engine.bind_noun::<Assignment>().expect("bind the noun");
    engine.register_rls(SampleRls).expect("register the rls");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the resolver");
    engine
        .register_projector(AssignmentProjector)
        .expect("register the projector");
    engine
        .register_cron(SampleCronJob::new(JOB, Schedule::EveryBeats(1), POD))
        .expect("register the cron job");

    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let mut tx = pool.begin().await.expect("open the staging transaction");
    for n in 0..BACKLOG {
        stage_outbox_row(&mut tx, &format!("stuck-{n}")).await;
    }
    tx.commit().await.expect("commit the backlog");

    nats.stop();
    tokio::time::sleep(Duration::from_millis(400)).await;

    let heartbeat_before = schema_heartbeat(&pool).await;
    let slots_before = completed_slots(&pool, JOB).await;

    tokio::time::sleep(OUTAGE_OBSERVE).await;

    let heartbeat_after = schema_heartbeat(&pool).await;
    let slots_after = completed_slots(&pool, JOB).await;

    assert!(
        heartbeat_after > heartbeat_before,
        "the schema-version heartbeat kept advancing while nats was down with a full outbox: \
         before {heartbeat_before}, after {heartbeat_after}"
    );
    assert!(
        slots_after > slots_before,
        "the cron kept completing leader slots while nats was down: before {slots_before}, \
         after {slots_after} — the outbox relay never froze the beat"
    );

    let reason = readiness_reason(&readiness);
    assert_eq!(
        reason.as_deref(),
        Some(service_engine::housekeeping::ready::REASON_NATS_UNREACHABLE),
        "readiness reflects the outage even while housekeeping keeps ticking"
    );

    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM service_engine.integration_outbox WHERE status = 'PENDING'",
    )
    .fetch_one(&pool)
    .await
    .expect("count the pending outbox rows");
    assert_eq!(
        pending, BACKLOG as i64,
        "every committed row still waits: none was abandoned as FAILED during the outage"
    );
    let failed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM service_engine.integration_outbox WHERE status = 'FAILED'",
    )
    .fetch_one(&pool)
    .await
    .expect("count the failed outbox rows");
    assert_eq!(failed, 0, "no outbox row is FAILED by an outage");

    running.abort();
    db.cleanup().await;
}

async fn schema_heartbeat(pool: &PgPool) -> DateTime<Utc> {
    sqlx::query_scalar("SELECT heartbeat FROM service_engine.schema_version WHERE singleton")
        .fetch_one(pool)
        .await
        .expect("read the schema-version heartbeat")
}

fn readiness_reason(readiness: &ReadinessHandle) -> Option<String> {
    match readiness.snapshot() {
        Readiness::NotReady { reason } => Some(reason),
        Readiness::Ready => None,
    }
}

async fn await_ready(readiness: &ReadinessHandle) {
    let deadline = tokio::time::Instant::now() + READY_WITHIN;
    loop {
        if readiness.snapshot() == Readiness::Ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "readiness never rose to UP while nats was reachable"
        );
        tokio::time::sleep(POLL).await;
    }
}
