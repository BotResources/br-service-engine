use std::time::Duration;

use br_core_integration::{Aggregate, Bc, EventCoords, PastFact};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::blobs::Uploaded;
use service_engine::nats::Nats;
use service_engine::pipeline::{Mutation, MutationInput, OutboundEvent, PostSave};
use service_engine::{
    BlobConfig, BlobPolicy, BlobPromotion, BlobRef, Engine, PostUpload, ReadinessHandle,
};
use sqlx::Row;
use uuid::Uuid;

use crate::infra::TestDb;
use crate::sample::blob::{AttachDoc, Attachment, attach_doc};
use crate::sample::blob_verified::{
    AttachVerifiedDoc, attach_verified_doc, fault_on_upload, refuse_upload,
};
use crate::sample::blob_view::{Doc, DocProjector};
use crate::sample::engine::engine_config;
use crate::sample::pipeline::SampleFault;
use crate::sample::principal::{
    SamplePrincipal, SamplePrincipalResolver, TENANT_SESSION_VAR, USER_SESSION_VAR,
};

#[derive(Debug, Deserialize)]
pub struct PromoteDoc {
    pub doc: Uuid,
    #[serde(default)]
    pub swallow_error: bool,
}

impl MutationInput for PromoteDoc {
    type Output = BlobPromotion;
    type Error = SampleFault;
    const NAME: &'static str = "promote_doc";
}

pub fn promote_doc<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: PromoteDoc,
) -> BoxFuture<'m, Result<BlobPromotion, SampleFault>> {
    Box::pin(async move {
        let reference: Uuid = sqlx::query_scalar("SELECT blob_ref FROM sample_doc WHERE id = $1")
            .bind(input.doc)
            .fetch_one(cx.connection())
            .await
            .map_err(service_engine::error::EngineError::from)?;
        match cx.promote_blob(BlobRef(reference)).await {
            Ok(answer) => Ok(answer),
            Err(_) if input.swallow_error => Ok(BlobPromotion::NotLanded),
            Err(error) => Err(error.into()),
        }
    })
}

#[derive(Debug, Deserialize)]
pub struct PromoteReference {
    pub reference: Uuid,
}

impl MutationInput for PromoteReference {
    type Output = BlobPromotion;
    type Error = SampleFault;
    const NAME: &'static str = "promote_reference";
}

pub fn promote_reference<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: PromoteReference,
) -> BoxFuture<'m, Result<BlobPromotion, SampleFault>> {
    Box::pin(async move { Ok(cx.promote_blob(BlobRef(input.reference)).await?) })
}

#[derive(Debug, Serialize)]
pub struct PromotionProbed {
    pub reference: Uuid,
    pub doc: Option<Uuid>,
    pub tenant_setting: Option<String>,
    pub user_setting: Option<String>,
    pub role_switched: bool,
}

impl OutboundEvent for PromotionProbed {
    fn coords(&self) -> EventCoords {
        EventCoords {
            producer: Bc::new("sample").unwrap(),
            aggregate: Aggregate::new("attachment").unwrap(),
            fact: PastFact::new("probed").unwrap(),
            version: 1,
        }
    }

    fn event_id(&self) -> Uuid {
        Uuid::now_v7()
    }
}

pub const PROBE_SUBJECT_PREFIX: &str = "integration.evt.sample.attachment.probed";

pub fn probe_on_upload<'a>(
    uploaded: Uploaded<'a>,
    ps: &'a mut PostSave<'_, '_>,
) -> BoxFuture<'a, Result<(), PostUpload>> {
    Box::pin(async move {
        let reference = uploaded.reference.as_uuid();
        let context = sqlx::query(
            "SELECT nullif(current_setting($1, true), '') AS tenant, \
                    nullif(current_setting($2, true), '') AS principal, \
                    current_user <> session_user AS switched, \
                    pg_sleep(0.15)::text AS slept",
        )
        .bind(TENANT_SESSION_VAR)
        .bind(USER_SESSION_VAR)
        .fetch_one(ps.connection())
        .await?;
        let doc: Option<Uuid> = sqlx::query_scalar("SELECT id FROM sample_doc WHERE blob_ref = $1")
            .bind(reference)
            .fetch_optional(ps.connection())
            .await?;
        if let Some(doc) = doc {
            ps.impact_caused::<Doc, _>(&doc, "attachment_probed")?;
        }
        ps.emit(PromotionProbed {
            reference,
            doc,
            tenant_setting: context.get("tenant"),
            user_setting: context.get("principal"),
            role_switched: context.get("switched"),
        })?;
        Ok(())
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotePolicy {
    None,
    Probe,
    Refuse,
    Fault,
}

#[allow(clippy::too_many_arguments)]
pub async fn boot_blob_promote_engine(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    blob: BlobConfig,
    policy: BlobPolicy,
    reaper_interval: Duration,
    upload_policy: PromotePolicy,
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
    .expect("the blob-promote engine boots under the low-privilege app role");
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
    engine
        .register_mutation::<PromoteDoc, _>(promote_doc)
        .expect("register the promote mutation");
    engine
        .register_mutation::<PromoteReference, _>(promote_reference)
        .expect("register the promote-by-reference mutation");
    let registered = match upload_policy {
        PromotePolicy::None => None,
        PromotePolicy::Probe => {
            Some(engine.register_post_upload_policy::<Attachment, _>(probe_on_upload))
        }
        PromotePolicy::Refuse => {
            Some(engine.register_post_upload_policy::<Attachment, _>(refuse_upload))
        }
        PromotePolicy::Fault => {
            Some(engine.register_post_upload_policy::<Attachment, _>(fault_on_upload))
        }
    };
    if let Some(registered) = registered {
        registered.expect("register the post-upload policy");
        engine
            .require_post_upload_policy::<Attachment>()
            .expect("declare the upload seam");
    }
    engine
}
