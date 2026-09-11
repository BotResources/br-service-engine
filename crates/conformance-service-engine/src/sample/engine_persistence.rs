use std::time::Duration;

use service_engine::Engine;
use service_engine::ReadinessHandle;
use service_engine::nats::Nats;

use crate::infra::TestDb;
use crate::sample::counter::{
    BumpCrud, BumpFull, BumpFullCmd, BumpLockless, BumpSoft, FullCounterProjector, OpenCrud,
    OpenFull, OpenLockless, OpenSoft, bump_crud, bump_full, bump_full_reaction, bump_lockless,
    bump_soft, open_crud, open_full, open_lockless, open_soft,
};
use crate::sample::engine::engine_config;
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver};

pub async fn boot_persistence_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod).with_lock_timeout(Duration::from_millis(300)),
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the persistence engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(FullCounterProjector)
        .expect("register the full-EDA counter projector, which auto-binds its noun");
    engine
        .register_mutation::<BumpCrud, _>(bump_crud)
        .expect("register the CRUD bump mutation");
    engine
        .register_mutation::<BumpSoft, _>(bump_soft)
        .expect("register the soft-EDA bump mutation");
    engine
        .register_mutation::<BumpFull, _>(bump_full)
        .expect("register the full-EDA bump mutation");
    engine
        .register_reaction::<BumpFullCmd, _, _>("sample-counter-bump", bump_full_reaction)
        .expect("register the coarse-disposition bump reaction");
    engine
}

pub async fn boot_serialization_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod).with_lock_timeout(Duration::from_secs(5)),
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the serialization engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(FullCounterProjector)
        .expect("register the full-EDA counter projector, which auto-binds its noun");
    engine
        .register_mutation::<BumpCrud, _>(bump_crud)
        .expect("register the CRUD bump mutation");
    engine
        .register_mutation::<BumpSoft, _>(bump_soft)
        .expect("register the soft-EDA bump mutation");
    engine
        .register_mutation::<BumpFull, _>(bump_full)
        .expect("register the full-EDA bump mutation");
    engine
        .register_mutation::<OpenCrud, _>(open_crud)
        .expect("register the CRUD open mutation");
    engine
        .register_mutation::<OpenSoft, _>(open_soft)
        .expect("register the soft-EDA open mutation");
    engine
        .register_mutation::<OpenFull, _>(open_full)
        .expect("register the full-EDA open mutation");
    engine
        .register_mutation::<BumpLockless, _>(bump_lockless)
        .expect("register the lock-less bump mutation");
    engine
        .register_mutation::<OpenLockless, _>(open_lockless)
        .expect("register the lock-less open mutation");
    engine
}
