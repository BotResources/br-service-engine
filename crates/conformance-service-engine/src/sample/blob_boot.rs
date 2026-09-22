use std::time::Duration;

use service_engine::ReadinessHandle;
use service_engine::nats::Nats;
use service_engine::{BlobConfig, BlobPolicy, Engine};

use crate::infra::TestDb;
use crate::sample::blob::{AttachDoc, Attachment, attach_doc};
use crate::sample::blob_verified::{
    AttachVerifiedDoc, attach_verified_doc, fault_on_upload, impact_doc_on_upload, refuse_upload,
};
use crate::sample::blob_view::DocProjector;
use crate::sample::engine::engine_config;
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver};

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

pub async fn boot_blob_fault_engine(
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
    .expect("the blob-fault engine boots under the low-privilege app role");
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
        .register_mutation::<AttachVerifiedDoc, _>(attach_verified_doc)
        .expect("register the verified attach mutation");
    engine
        .register_post_upload_policy::<Attachment, _>(fault_on_upload)
        .expect("register the faulting post-upload policy");
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
