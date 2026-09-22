use std::time::Duration;

use service_engine::ReadinessHandle;
use service_engine::nats::Nats;
use service_engine::{BlobConfig, BlobPolicy, Engine};

use crate::infra::TestDb;
use crate::sample::blob::{
    AttachDoc, AttachOwnedDoc, Attachment, DeleteDoc, DetachDoc, RepointDoc, attach_doc,
    attach_owned_doc, delete_doc, detach_doc, repoint_doc,
};
use crate::sample::blob_verified::{AttachVerifiedDoc, attach_verified_doc};
use crate::sample::blob_view::DocProjector;
use crate::sample::engine::engine_config;
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver};

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
