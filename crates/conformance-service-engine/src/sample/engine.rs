use std::time::Duration;

use service_engine::ReadinessHandle;
use service_engine::config::EngineConfig;
use service_engine::cron::Schedule;
use service_engine::name::{ChannelName, PodId, RelayName};
use service_engine::nats::Nats;
use service_engine::{BlobConfig, BlobPolicy, Engine};

use crate::infra::TestDb;
use crate::sample::assignment::{Assignment, AssignmentProjector};
use crate::sample::blob::{
    AttachDoc, AttachOwnedDoc, Attachment, DeleteDoc, DetachDoc, RepointDoc, attach_doc,
    attach_owned_doc, delete_doc, detach_doc, repoint_doc,
};
use crate::sample::blob_view::DocProjector;
use crate::sample::cron::SampleCronJob;
use crate::sample::mirror::directory_mirror;
use crate::sample::note::{Note, NoteProjector};
use crate::sample::offer::{MintThenReject, WidgetOffer, mint_then_reject};
use crate::sample::pipeline::{
    CloseWidget, CreateWidget, ImportWidgets, LockWidget, MintSecret, ScheduleCreate, close_widget,
    create_widget, import_widgets, lock_widget, mint_secret, schedule_create,
};
use crate::sample::presence::Typing;
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver, SampleRls};
use crate::sample::relays::RowClaimSampleRelay;
use crate::sample::stream::NoteBody;
use crate::sample::widget::WidgetProjector;

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

pub async fn boot_sample_engine(
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

pub async fn boot_blob_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    blob: BlobConfig,
    policy: BlobPolicy,
    reaper_interval: Duration,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod)
            .with_blob_storage(blob)
            .with_blob_reaper_interval(reaper_interval),
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the blob engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(DocProjector)
        .expect("register the doc projector, which auto-binds the doc noun");
    engine
        .register_blobs::<Attachment>(policy)
        .expect("register the attachment blob kind");
    engine
        .register_mutation::<AttachDoc, _>(attach_doc)
        .expect("register the attach mutation");
    engine
        .register_mutation::<AttachOwnedDoc, _>(attach_owned_doc)
        .expect("register the attach-owned mutation");
    engine
        .register_mutation::<DetachDoc, _>(detach_doc)
        .expect("register the detach mutation");
    engine
        .register_mutation::<RepointDoc, _>(repoint_doc)
        .expect("register the repoint mutation");
    engine
        .register_mutation::<DeleteDoc, _>(delete_doc)
        .expect("register the delete mutation");
    engine
}

pub async fn boot_pipeline_engine(
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
    .expect("the pipeline engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector, which auto-binds the widget noun");
    engine
        .register_mutation::<CloseWidget, _>(close_widget)
        .expect("register the close mutation");
    engine
        .register_mutation::<MintSecret, _>(mint_secret)
        .expect("register the mint mutation");
    engine
        .register_mutation::<ScheduleCreate, _>(schedule_create)
        .expect("register the schedule mutation");
    engine
        .register_bulk::<ImportWidgets, _>(import_widgets)
        .expect("register the import bulk");
    engine
        .register_reaction::<CreateWidget, _, _>("sample-create-widget", create_widget)
        .expect("register the create-widget reaction");
    engine
        .register_reaction::<LockWidget, _, _>("sample-lock-widget", lock_widget)
        .expect("register the lock-widget reaction");
    engine
}

pub async fn boot_offer_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
) -> Engine<SamplePrincipal> {
    boot_offer_engine_reconciling(db, nats, channel, pod, Duration::from_secs(300)).await
}

pub async fn boot_offer_engine_reconciling(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    reconcile: Duration,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod)
            .with_lock_timeout(Duration::from_millis(300))
            .with_offer_reconcile(reconcile),
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the offer engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector, which auto-binds the widget noun");
    engine
        .register_mutation::<MintSecret, _>(mint_secret)
        .expect("register the mint mutation");
    engine
        .register_mutation::<CloseWidget, _>(close_widget)
        .expect("register the close mutation");
    engine
        .register_mutation::<MintThenReject, _>(mint_then_reject)
        .expect("register the mint-then-reject mutation");
    engine
        .register_offer::<WidgetOffer>()
        .expect("register the widget offer");
    engine
}
