use std::time::Duration;

use service_engine::Engine;
use service_engine::ReadinessHandle;
use service_engine::config::EngineConfig;
use service_engine::cron::Schedule;
use service_engine::name::{ChannelName, PodId, RelayName};
use service_engine::nats::Nats;

use crate::infra::TestDb;
use crate::sample::assignment::{Assignment, AssignmentProjector};
use crate::sample::cron::SampleCronJob;
use crate::sample::mirror::directory_mirror;
use crate::sample::note::{Note, NoteProjector};
use crate::sample::presence::{Cursor, Typing};
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver, SampleRls};
use crate::sample::relays::RowClaimSampleRelay;
use crate::sample::stream::NoteBody;

pub const SAMPLE_RELAY: RelayName = RelayName::from_static("sample_rows");
pub const SAMPLE_JOB: &str = "sample_heartbeat";
pub const SAMPLE_SERVICE: &str = "sample";

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
    nats: Nats,
    channel: &str,
    pod: &str,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod),
        db.app_pool().clone(),
        nats,
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

pub async fn boot_presence_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    service: &str,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod).with_service(service),
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the presence engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_presence::<Typing>(Duration::from_secs(30))
        .expect("register the typing presence lane");
    engine
}

pub const FAST_PRESENCE_TTL: Duration = Duration::from_secs(2);
pub const SLOW_PRESENCE_TTL: Duration = Duration::from_secs(6);

pub async fn boot_dual_presence_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    service: &str,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod).with_service(service),
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the dual-lane presence engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_presence::<Typing>(FAST_PRESENCE_TTL)
        .expect("register the fast typing presence lane");
    engine
        .register_presence::<Cursor>(SLOW_PRESENCE_TTL)
        .expect("register the slow cursor presence lane");
    engine
}

pub async fn boot_sample_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod).with_service(SAMPLE_SERVICE),
        db.app_pool().clone(),
        nats,
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
    engine
        .register_mirror(directory_mirror())
        .expect("register the directory mirror");
    engine
}
