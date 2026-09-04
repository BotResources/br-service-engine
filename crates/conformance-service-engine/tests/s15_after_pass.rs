use std::time::Duration;

use br_util_axum_readiness::{Readiness, ReadinessHandle};
use conformance_service_engine::infra::listener::engine_config;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::assignment::{Assignment, AssignmentProjector};
use conformance_service_engine::sample::principal::{
    SamplePrincipal, SamplePrincipalResolver, SampleRls,
};
use conformance_service_engine::sample::render::assignment;
use conformance_service_engine::sample::{RowClaimSampleRelay, SAMPLE_RELAY};
use service_engine::Engine;
use service_engine::impact::{Dims, Impact};
use sqlx::PgPool;
use uuid::Uuid;

const CHANNEL: &str = "se_after_pass_impact";
const READY_WITHIN: Duration = Duration::from_secs(25);
const POLL: Duration = Duration::from_millis(25);

#[tokio::test]
async fn s15_a_row_staged_with_an_impact_is_drained_after_the_render_pass_before_the_long_beat() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;

    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let subject = assignment(&pool, home, "alpha").await;

    let config = engine_config(CHANNEL, "pod-after-pass")
        .with_beat(Duration::from_secs(30))
        .with_lease(Duration::from_secs(45));
    let mut engine =
        Engine::<SamplePrincipal>::boot(config, pool.clone(), fabric, ReadinessHandle::ready())
            .await
            .expect("the engine boots under the low-privilege app role");
    engine.bind_noun::<Assignment>().expect("bind the noun");
    engine.register_rls(SampleRls).expect("register RLS");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the resolver");
    engine
        .register_projector(AssignmentProjector)
        .expect("register the projector");
    engine
        .register_relay(RowClaimSampleRelay::new(SAMPLE_RELAY, Duration::ZERO))
        .expect("register the RowClaim relay");

    let readiness = engine.readiness();
    let transport = engine.transport_arc();
    let shutdown = engine.shutdown_handle();
    let running = tokio::spawn(engine.run());

    await_ready(&readiness).await;

    let mut tx = pool.begin().await.expect("a write transaction");
    let row = Uuid::now_v7();
    sqlx::query("INSERT INTO sample_relay_row (id) VALUES ($1)")
        .bind(row)
        .execute(&mut *tx)
        .await
        .expect("stage an outbox-style row");
    let impact = Impact::resource::<Assignment>(&subject, Dims::EMPTY).expect("the key encodes");
    transport
        .stage_in(&mut tx, std::slice::from_ref(&impact))
        .await
        .expect("stage the impact in the same transaction as the row");
    tx.commit().await.expect("commit the gesture");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        if unclaimed(&pool).await == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the row staged with the impact was not drained; the periodic beat is 30s away, so \
             only Engine::run wiring the after-pass drain into the beat can claim it this fast"
        );
        tokio::time::sleep(POLL).await;
    }

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok on a clean shutdown");

    drop(nats);
    db.cleanup().await;
}

async fn unclaimed(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM sample_relay_row WHERE claimed_at IS NULL")
        .fetch_one(pool)
        .await
        .expect("count unclaimed rows")
}

async fn await_ready(readiness: &ReadinessHandle) {
    let deadline = tokio::time::Instant::now() + READY_WITHIN;
    loop {
        if readiness.snapshot() == Readiness::Ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "readiness never rose to UP"
        );
        tokio::time::sleep(POLL).await;
    }
}
