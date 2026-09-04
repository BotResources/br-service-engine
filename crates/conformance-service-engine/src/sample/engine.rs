use std::time::Duration;

use br_util_axum_readiness::ReadinessHandle;
use br_util_nats_fabric::Fabric;
use service_engine::Engine;
use service_engine::config::EngineConfig;
use service_engine::cron::Schedule;
use service_engine::name::{ChannelName, PodId, RelayName};

use crate::infra::TestDb;
use crate::sample::assignment::{Assignment, AssignmentProjector};
use crate::sample::cron::SampleCronJob;
use crate::sample::mirror::directory_mirror_handle;
use crate::sample::note::{Note, NoteProjector};
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver, SampleRls};
use crate::sample::relays::RowClaimSampleRelay;
use crate::sample::stream::NoteBody;

pub const SAMPLE_RELAY: RelayName = RelayName::from_static("sample_rows");
pub const SAMPLE_JOB: &str = "sample_heartbeat";

pub fn engine_config(channel: &str, pod: &str) -> EngineConfig {
    EngineConfig::new(
        ChannelName::new(channel).expect("a valid notify channel"),
        PodId::new(pod).expect("a valid pod id"),
    )
    .with_window(Duration::from_millis(30))
    .with_beat(Duration::from_millis(80))
    .with_lease(Duration::from_secs(5))
}

pub async fn boot_render_engine(
    db: &TestDb,
    fabric: Fabric,
    channel: &str,
    pod: &str,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod),
        db.app_pool().clone(),
        fabric,
        ReadinessHandle::ready(),
    )
    .await
    .expect("a render-only engine boots under the low-privilege app role");
    engine
        .bind_noun::<Assignment>()
        .expect("bind the assignment noun");
    engine
        .register_rls(SampleRls)
        .expect("register the RLS applier");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(AssignmentProjector)
        .expect("register the assignment projector");
    engine
}

pub async fn boot_sample_engine(
    db: &TestDb,
    fabric: Fabric,
    channel: &str,
    pod: &str,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod),
        db.app_pool().clone(),
        fabric.clone(),
        ReadinessHandle::ready(),
    )
    .await
    .expect("the sample engine boots under the low-privilege app role");
    engine
        .bind_noun::<Assignment>()
        .expect("bind the assignment noun");
    engine.bind_noun::<Note>().expect("bind the note noun");
    engine
        .register_rls(SampleRls)
        .expect("register the RLS applier");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(AssignmentProjector)
        .expect("register the assignment projector");
    engine
        .register_projector(NoteProjector)
        .expect("register the note projector");
    engine
        .register_accumulator(NoteBody)
        .expect("register the note accumulator");
    engine
        .register_relay(RowClaimSampleRelay::new(SAMPLE_RELAY, Duration::ZERO))
        .expect("register the sample relay");
    engine
        .register_cron(SampleCronJob::new(SAMPLE_JOB, Schedule::EveryBeats(1), pod))
        .expect("register the sample cron job");
    let transport = engine.transport_arc();
    engine
        .register_mirror(directory_mirror_handle(
            fabric,
            db.app_pool().clone(),
            transport,
        ))
        .expect("register the directory mirror");
    engine
}
