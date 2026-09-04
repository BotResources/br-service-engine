use std::time::Duration;

use br_util_axum_readiness::{Readiness, ReadinessHandle};
use br_util_nats_fabric::Fabric;
use conformance_service_engine::infra::TestDb;
use conformance_service_engine::sample::assignment::Assignment;
use conformance_service_engine::sample::note::Note;
use conformance_service_engine::sample::principal::{
    SamplePrincipal, SamplePrincipalResolver, SampleRls,
};
use service_engine::Engine;
use service_engine::config::EngineConfig;
use service_engine::impact::Impact;
use service_engine::projector::Projector;
use service_engine::transport::ImpactTransport;
use sqlx::PgPool;

pub const READY_WITHIN: Duration = Duration::from_secs(25);
pub const SOON: Duration = Duration::from_secs(15);
pub const SILENCE: Duration = Duration::from_millis(300);
pub const POLL: Duration = Duration::from_millis(25);

pub async fn await_ready(readiness: &ReadinessHandle) {
    let deadline = tokio::time::Instant::now() + READY_WITHIN;
    loop {
        if readiness.snapshot() == Readiness::Ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "readiness never rose to UP through Engine::run"
        );
        tokio::time::sleep(POLL).await;
    }
}

pub async fn stage(pool: &PgPool, transport: &dyn ImpactTransport, impacts: &[Impact]) {
    let mut tx = pool.begin().await.expect("open a write transaction");
    transport
        .stage_in(&mut tx, impacts)
        .await
        .expect("stage the impacts over the real transport");
    tx.commit().await.expect("commit the staging transaction");
}

pub async fn spy_engine<Pr: Projector<Principal = SamplePrincipal>>(
    db: &TestDb,
    fabric: Fabric,
    config: EngineConfig,
    projector: Pr,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::<SamplePrincipal>::boot(
        config,
        db.app_pool().clone(),
        fabric,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the engine boots under the low-privilege app role");
    engine.bind_noun::<Assignment>().expect("bind assignment");
    engine.bind_noun::<Note>().expect("bind note");
    engine
        .register_rls(SampleRls)
        .expect("register the RLS applier");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(projector)
        .expect("register the sample projector");
    engine
}
