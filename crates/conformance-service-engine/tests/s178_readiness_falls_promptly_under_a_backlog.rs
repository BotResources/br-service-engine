use std::time::Duration;

use conformance_service_engine::infra::listener::engine_config;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::assignment::{Assignment, AssignmentProjector};
use conformance_service_engine::sample::principal::{SamplePrincipalResolver, SampleRls};
use conformance_service_engine::sample::stage_outbox_row;
use service_engine::Engine;
use service_engine::housekeeping::ready::REASON_NATS_UNREACHABLE;
use service_engine::{Readiness, ReadinessHandle};

const BACKLOG: usize = 300;
const BEAT: Duration = Duration::from_millis(100);
const NATS_GRACE: Duration = Duration::from_secs(2);
const READY_WITHIN: Duration = Duration::from_secs(25);
const DETECTION_MARGIN: Duration = Duration::from_secs(3);
const POLL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn s178_a_pending_backlog_does_not_delay_the_nats_down_verdict() {
    let db = TestDb::fresh().await;
    let mut nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let config = engine_config("se_s178_backlog_down", "pod-s178")
        .with_beat(BEAT)
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

    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let stopped_at = tokio::time::Instant::now();
    nats.stop();

    let mut tx = pool.begin().await.expect("open the staging transaction");
    for n in 0..BACKLOG {
        stage_outbox_row(&mut tx, &format!("backlog-{n}")).await;
    }
    tx.commit().await.expect("commit the backlog while nats is down");

    let reason = await_not_ready(&readiness, NATS_GRACE + DETECTION_MARGIN).await;
    let took = stopped_at.elapsed();
    assert_eq!(
        reason, REASON_NATS_UNREACHABLE,
        "the pod goes DOWN for the nats reason, not some frozen-beat artefact"
    );
    assert!(
        took <= NATS_GRACE + DETECTION_MARGIN,
        "readiness fell within nats_grace plus a small detection margin ({took:?}); a beat frozen \
         on the {BACKLOG}-row outbox would have held it UP for minutes"
    );

    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM integration_outbox WHERE status = 'PENDING'")
            .fetch_one(&pool)
            .await
            .expect("count the pending outbox rows");
    assert_eq!(
        pending, BACKLOG as i64,
        "the whole backlog is still pending under the outage; the DOWN verdict is not the relay \
         having quietly drained it"
    );

    running.abort();
    db.cleanup().await;
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

async fn await_not_ready(readiness: &ReadinessHandle, within: Duration) -> String {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        if let Readiness::NotReady { reason } = readiness.snapshot() {
            return reason;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "readiness stayed UP past nats_grace plus the detection margin under a full outbox"
        );
        tokio::time::sleep(POLL).await;
    }
}
