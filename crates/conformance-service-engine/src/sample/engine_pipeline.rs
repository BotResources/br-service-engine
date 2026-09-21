use std::time::Duration;

use service_engine::ReadinessHandle;
use service_engine::nats::Nats;
use service_engine::{BlobConfig, BlobPolicy, Engine};

use crate::infra::TestDb;
use crate::sample::blob::{
    AttachDoc, AttachOwnedDoc, Attachment, DeleteDoc, DetachDoc, RepointDoc, attach_doc,
    attach_owned_doc, delete_doc, detach_doc, repoint_doc,
};
use crate::sample::blob_verified::{
    AttachVerifiedDoc, attach_verified_doc, impact_doc_on_upload, refuse_upload,
};
use crate::sample::blob_view::DocProjector;
use crate::sample::engine::engine_config;
use crate::sample::offer::{MintThenReject, WidgetOffer, mint_then_reject};
use crate::sample::pipeline::{
    CloseWidget, CreateWidget, DeleteWidget, DetonateWidget, ImportWidgets, LockWidget, MintSecret,
    RelabelBoth, RelabelWidget, ScheduleCreate, close_widget, create_widget, delete_widget,
    detonate_widget, import_widgets, lock_widget, mint_secret, refuse_breach_label,
    refuse_pinned_delete, refuse_unchanged_label, relabel_both, relabel_widget, schedule_create,
};
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver};
use crate::sample::widget::{WidgetProjector, WidgetRow};
use crate::sample::widget_tag::{
    DeleteWidgetTag, SetWidgetTag, WidgetTag, delete_widget_tag, set_widget_tag,
};

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
        .register_mutation::<AttachVerifiedDoc, _>(attach_verified_doc)
        .expect("register the verified attach mutation");
    engine
}

#[allow(clippy::too_many_arguments)]
pub async fn boot_blob_policy_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    blob: BlobConfig,
    policy: BlobPolicy,
    reaper_interval: Duration,
    refuse: bool,
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
    .expect("the blob-policy engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(DocProjector)
        .expect("register the doc projector");
    engine
        .register_blobs::<Attachment>(policy)
        .expect("register the attachment blob kind");
    engine
        .register_mutation::<AttachDoc, _>(attach_doc)
        .expect("register the attach mutation");
    engine
        .register_mutation::<AttachVerifiedDoc, _>(attach_verified_doc)
        .expect("register the verified attach mutation");
    if refuse {
        engine
            .register_post_upload_policy::<Attachment, _>(refuse_upload)
            .expect("register the refusing post-upload policy");
    } else {
        engine
            .register_post_upload_policy::<Attachment, _>(impact_doc_on_upload)
            .expect("register the impacting post-upload policy");
    }
    engine
        .require_post_upload_policy::<Attachment>()
        .expect("declare the upload seam");
    engine
}

pub async fn boot_unhonoured_upload_seam_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    blob: BlobConfig,
    policy: BlobPolicy,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod).with_blob_storage(blob),
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the engine boots");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(DocProjector)
        .expect("register the doc projector");
    engine
        .register_blobs::<Attachment>(policy)
        .expect("register the attachment blob kind");
    engine
        .require_post_upload_policy::<Attachment>()
        .expect("declare the upload seam without registering a policy");
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
        .register_mutation::<RelabelBoth, _>(relabel_both)
        .expect("register the relabel-both mutation (load_many over two widgets)");
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

pub async fn boot_panic_engine(
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
    .expect("the panic engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector, which auto-binds the widget noun");
    engine
        .register_reaction::<DetonateWidget, _, _>("sample-detonate-widget", detonate_widget)
        .expect("register the detonate reaction whose handler can panic");
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
    boot_offer_engine_leased(
        db,
        nats,
        channel,
        pod,
        reconcile,
        service_engine::config::DEFAULT_LEASE,
    )
    .await
}

pub async fn boot_offer_engine_leased(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    reconcile: Duration,
    lease: Duration,
) -> Engine<SamplePrincipal> {
    let mut engine = Engine::boot(
        engine_config(channel, pod)
            .with_lock_timeout(Duration::from_millis(300))
            .with_offer_reconcile(reconcile)
            .with_lease(lease),
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
        .register_mutation::<RelabelWidget, _>(relabel_widget)
        .expect("register the relabel mutation");
    engine
        .register_mutation::<DeleteWidget, _>(delete_widget)
        .expect("register the delete mutation");
    engine
        .register_mutation::<MintThenReject, _>(mint_then_reject)
        .expect("register the mint-then-reject mutation");
    engine
        .register_offer::<WidgetOffer>()
        .expect("register the widget offer");
    engine
}

pub async fn boot_policy_engine(
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
    .expect("the policy engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector, which auto-binds the widget noun");
    engine
        .require_post_save_policy::<WidgetRow>()
        .expect("declare the widget subject to a post-save policy");
    engine
        .register_post_save_policy::<WidgetRow, _>(refuse_breach_label)
        .expect("honour the subjection with the breach-label policy");
    engine
        .register_mutation::<MintSecret, _>(mint_secret)
        .expect("register the mint mutation");
    engine
        .register_mutation::<RelabelWidget, _>(relabel_widget)
        .expect("register the relabel mutation");
    engine
        .register_mutation::<RelabelBoth, _>(relabel_both)
        .expect("register the relabel-both mutation");
    engine
}

pub async fn boot_unhonoured_seam_engine(
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
    .expect("the engine boots; the unhonoured seam is caught by run, not boot");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector, which auto-binds the widget noun");
    engine
        .require_post_save_policy::<WidgetRow>()
        .expect("declare the subjection but deliberately never honour it");
    engine
}

pub async fn boot_transition_policy_engine(
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
    .expect("the transition-policy engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector, which auto-binds the widget noun");
    engine
        .require_post_save_policy::<WidgetRow>()
        .expect("declare the widget subject to a post-save policy");
    engine
        .register_post_save_policy::<WidgetRow, _>(refuse_unchanged_label)
        .expect("honour the subjection with the unchanged-label policy");
    engine
        .register_mutation::<MintSecret, _>(mint_secret)
        .expect("register the mint mutation");
    engine
        .register_mutation::<RelabelWidget, _>(relabel_widget)
        .expect("register the relabel mutation");
    engine
}

pub async fn boot_delete_policy_engine(
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
    .expect("the delete-policy engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector, which auto-binds the widget noun");
    engine
        .require_post_delete_policy::<WidgetRow>()
        .expect("declare the widget subject to a post-delete policy");
    engine
        .register_post_delete_policy::<WidgetRow, _>(refuse_pinned_delete)
        .expect("honour the subjection with the pinned-delete policy");
    engine
        .register_mutation::<MintSecret, _>(mint_secret)
        .expect("register the mint mutation");
    engine
        .register_mutation::<DeleteWidget, _>(delete_widget)
        .expect("register the delete mutation");
    engine
}

pub async fn boot_offer_trigger_engine(
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
    .expect("the offer-trigger engine boots under the low-privilege app role");
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
        .register_mutation::<SetWidgetTag, _>(set_widget_tag)
        .expect("register the set-tag mutation");
    engine
        .register_mutation::<DeleteWidgetTag, _>(delete_widget_tag)
        .expect("register the delete-tag mutation");
    engine
        .register_offer::<WidgetOffer>()
        .expect("register the widget offer");
    engine
        .register_offer_trigger::<WidgetOffer, WidgetTag>()
        .expect("register the widget-tag trigger for the widget offer");
    engine
}
