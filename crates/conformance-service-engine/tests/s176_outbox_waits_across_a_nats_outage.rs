use std::time::Duration;

use conformance_service_engine::infra::listener::engine_config;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::assignment::{Assignment, AssignmentProjector};
use conformance_service_engine::sample::outbox::row_status;
use conformance_service_engine::sample::principal::{SamplePrincipalResolver, SampleRls};
use conformance_service_engine::sample::{delivered_event_ids, stage_outbox_row};
use service_engine::Engine;
use service_engine::{Readiness, ReadinessHandle};
use sqlx::PgPool;
use uuid::Uuid;

const BACKLOG: usize = 6;
const NATS_GRACE: Duration = Duration::from_secs(2);
const READY_WITHIN: Duration = Duration::from_secs(25);
const PUBLISHED_WITHIN: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(100);

#[tokio::test]
async fn s176_committed_outbox_rows_wait_out_a_nats_outage_and_deliver_once_on_return() {
    let db = TestDb::fresh().await;
    let mut nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let config = engine_config("se_s176_outbox_wait", "pod-s176")
        .with_beat(Duration::from_millis(100))
        .with_nats_grace(NATS_GRACE);
    let readiness = ReadinessHandle::ready();
    let mut engine = Engine::boot(config, pool.clone(), fabric.clone(), readiness.clone())
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

    nats.stop();

    let mut staged = Vec::with_capacity(BACKLOG);
    let mut tx = pool.begin().await.expect("open the staging transaction");
    for n in 0..BACKLOG {
        staged.push(stage_outbox_row(&mut tx, &format!("wait-{n}")).await);
    }
    tx.commit()
        .await
        .expect("commit the backlog while nats is down");

    tokio::time::sleep(Duration::from_millis(600)).await;
    for id in &staged {
        assert_eq!(
            row_status(&pool, *id).await,
            "PENDING",
            "a committed outbox row waits while the broker is unreachable, it is never FAILED"
        );
    }
    assert_eq!(
        outbox_source_dead_letters(&pool).await,
        0,
        "an outage is not poison: no outbox row is dead-lettered while nats is down"
    );

    nats.restart().await;
    await_all_published(&pool, &staged).await;

    let mut delivered = delivered_event_ids(&fabric, "se-observer-s176").await;
    delivered.sort();
    let mut expected = staged.clone();
    expected.sort();
    assert_eq!(
        delivered, expected,
        "every waiting row is delivered exactly once after the broker returns"
    );
    assert_eq!(
        outbox_source_dead_letters(&pool).await,
        0,
        "nothing is dead-lettered: the outage drained clean on return"
    );

    running.abort();
    db.cleanup().await;
}

async fn outbox_source_dead_letters(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.dead_letter WHERE source = 'outbox'")
        .fetch_one(pool)
        .await
        .expect("read the outbox dead letters")
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

async fn await_all_published(pool: &PgPool, staged: &[Uuid]) {
    let deadline = tokio::time::Instant::now() + PUBLISHED_WITHIN;
    loop {
        let mut all = true;
        for id in staged {
            if row_status(pool, *id).await != "PUBLISHED" {
                all = false;
                break;
            }
        }
        if all {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the waiting outbox rows never drained after the broker came back"
        );
        tokio::time::sleep(POLL).await;
    }
}
