#[allow(dead_code)]
mod engine_twin;

use std::time::{Duration, Instant};

use conformance_service_engine::inbound_support::{SampleCommand, publish, sample_handler};
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::SamplePrincipal;
use conformance_service_engine::sample::engine::engine_config;
use engine_twin::await_ready;
use service_engine::inbound::StubPayload;
use service_engine::{Engine, Readiness, ReadinessHandle};
use sqlx::PgPool;
use uuid::Uuid;

const CHANNEL: &str = "se_s160_reaction";
const SERVICE: &str = "s151reaction";

async fn claimed(pool: &PgPool, id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.message_claim WHERE message_id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("count the claim rows for one message")
}

async fn await_claimed(pool: &PgPool, id: Uuid) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if claimed(pool, id).await == 1 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the supervised reaction consumer never processed the command"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn await_not_ready(readiness: &ReadinessHandle) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if readiness.snapshot() != Readiness::Ready {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "readiness never fell while the pod was deaf to a killed broker"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn s160_a_supervised_reaction_consumer_falls_deaf_then_recovers_across_a_broker_restart() {
    let db = TestDb::fresh().await;
    let mut nats = TestNats::spawn().await;
    nats.provision().await;

    let config = engine_config(CHANNEL, "pod-s151")
        .with_service(SERVICE)
        .with_nats_grace(Duration::from_secs(1))
        .with_beat(Duration::from_millis(200));
    let mut engine = Engine::<SamplePrincipal>::boot(
        config,
        db.app_pool().clone(),
        nats.nats().await,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the pod boots under the low-privilege app role");
    engine
        .register_reaction::<SampleCommand, _, _>("s151-work", sample_handler)
        .expect("register_reaction records the reaction and derives its subscription");
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let first = Uuid::now_v7();
    publish(&nats.nats().await, first, &StubPayload::commit(), None).await;
    await_claimed(db.app_pool(), first).await;

    nats.stop();
    await_not_ready(&readiness).await;

    nats.restart().await;
    await_ready(&readiness).await;

    let second = Uuid::now_v7();
    publish(&nats.nats().await, second, &StubPayload::commit(), None).await;
    await_claimed(db.app_pool(), second).await;

    shutdown.notify_one();
    running
        .await
        .expect("the pod task joins")
        .expect("the pod run returns Ok after a supervised recovery");
    drop(nats);
    db.cleanup().await;
}
