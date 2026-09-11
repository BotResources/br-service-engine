use std::time::Duration;

use service_engine::ReadinessHandle;
use service_engine::nats::Nats;
use service_engine::{BlobConfig, BlobPolicy, Engine};

use crate::infra::TestDb;
use crate::sample::engine::engine_config;
use crate::sample::erase::note::{EraseNoteOffer, EraseNoteProjector, EraseNoteStream};
use crate::sample::erase::secret::SecretEraser;
use crate::sample::erase::slice::{
    AttachNoteBlob, FailingEraser, LedgerEraser, MemoEraser, NoteDeleteEraser, NoteEraser,
    attach_note_blob,
};
use crate::sample::presence::Typing;
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver};

async fn boot(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    service: &str,
) -> Engine<SamplePrincipal> {
    Engine::boot(
        engine_config(channel, pod)
            .with_service(service)
            .with_lock_timeout(Duration::from_millis(500)),
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the erase engine boots under the low-privilege app role")
}

fn register_common(engine: &mut Engine<SamplePrincipal>) {
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(EraseNoteProjector)
        .expect("register the erase-note projector");
    engine
        .register_offer::<EraseNoteOffer>()
        .expect("register the erase-note offer");
    engine
        .register_erasable(NoteEraser)
        .expect("register the CRUD note eraser");
    engine
        .register_erasable(MemoEraser)
        .expect("register the soft-EDA memo eraser");
    engine
        .register_erasable(LedgerEraser)
        .expect("register the full-EDA ledger eraser");
}

pub async fn boot_erase_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    service: &str,
) -> Engine<SamplePrincipal> {
    let mut engine = boot(db, nats, channel, pod, service).await;
    register_common(&mut engine);
    engine
}

pub async fn boot_erase_lane_a_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    service: &str,
) -> Engine<SamplePrincipal> {
    let mut engine = boot(db, nats, channel, pod, service).await;
    register_common(&mut engine);
    engine
        .register_accumulator(EraseNoteStream)
        .expect("register the erase-note accumulator so lane A binds its STREAMING stream");
    engine
}

pub async fn boot_erase_via_delete_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    service: &str,
) -> Engine<SamplePrincipal> {
    let mut engine = boot(db, nats, channel, pod, service).await;
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_offer::<EraseNoteOffer>()
        .expect("register the erase-note offer");
    engine
        .register_erasable(NoteDeleteEraser)
        .expect("register the cx.delete-based note eraser");
    engine
}

pub async fn boot_failing_erase_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    service: &str,
) -> Engine<SamplePrincipal> {
    let mut engine = boot(db, nats, channel, pod, service).await;
    register_common(&mut engine);
    engine
        .register_erasable(FailingEraser)
        .expect("register the failing eraser after the real ones");
    engine
}

pub async fn boot_erase_strict_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    service: &str,
) -> Engine<SamplePrincipal> {
    let mut engine = boot(db, nats, channel, pod, service).await;
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_erasable(SecretEraser)
        .expect("register the strict-RLS secret eraser");
    engine
}

pub async fn boot_erase_presence_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    service: &str,
) -> Engine<SamplePrincipal> {
    let mut engine = boot(db, nats, channel, pod, service).await;
    register_common(&mut engine);
    engine
        .register_presence::<Typing>(Duration::from_secs(30))
        .expect("register the typing presence lane");
    engine
}

pub async fn boot_erase_blob_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    service: &str,
    blob: BlobConfig,
    policy: BlobPolicy,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod)
            .with_service(service)
            .with_lock_timeout(Duration::from_millis(500))
            .with_blob_storage(blob)
            .with_blob_reaper_interval(Duration::from_secs(3600)),
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the blob erase engine boots under the low-privilege app role");
    register_common(&mut engine);
    engine
        .register_blobs::<crate::sample::blob::Attachment>(policy)
        .expect("register the attachment blob kind");
    engine
        .register_mutation::<AttachNoteBlob, _>(attach_note_blob)
        .expect("register the attach-note-blob mutation");
    engine
}
