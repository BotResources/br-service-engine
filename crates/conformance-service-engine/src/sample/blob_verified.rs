use br_core_integration::{Aggregate, Bc, EventCoords, PastFact};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::UploadUrl;
use service_engine::blobs::{Sha256Digest, UploadExpectation, Uploaded};
use service_engine::gate::Reason;
use service_engine::pipeline::{
    Mutation, MutationInput, OneShot, OutboundEvent, PostSave, Refused,
};
use sqlx::Row;
use uuid::Uuid;

use crate::sample::blob::{Attachment, DocRow};
use crate::sample::blob_view::Doc;
use crate::sample::pipeline::SampleFault;
use crate::sample::principal::SamplePrincipal;

pub const UPLOAD_REFUSED: Reason = Reason::new("UPLOAD_REFUSED");

#[derive(Debug, Deserialize)]
pub struct AttachVerifiedDoc {
    pub id: Uuid,
    pub tenant: Uuid,
    pub name: String,
    pub content_type: String,
    pub size: u64,
    pub sha256_hex: String,
}

impl MutationInput for AttachVerifiedDoc {
    type Output = OneShot<UploadUrl>;
    type Error = SampleFault;
    const NAME: &'static str = "attach_verified_doc";
}

pub fn attach_verified_doc<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: AttachVerifiedDoc,
) -> BoxFuture<'m, Result<OneShot<UploadUrl>, SampleFault>> {
    Box::pin(async move {
        let sha256 = Sha256Digest::from_hex(&input.sha256_hex)
            .map_err(|error| SampleFault::Store(error.to_string()))?;
        let blob = cx.blob_verified::<Attachment>(
            input.name.clone(),
            input.content_type,
            UploadExpectation::new(input.size, sha256),
        )?;
        let doc = DocRow {
            id: input.id,
            tenant_id: input.tenant,
            name: input.name,
            blob_ref: Some(blob.reference().as_uuid()),
        };
        cx.create(&doc).await?;
        cx.impact_caused::<Doc, _>(&doc.id, "attached")?;
        Ok(OneShot(blob.upload_url()))
    })
}

pub fn impact_doc_on_upload<'a>(
    uploaded: Uploaded<'a>,
    ps: &'a mut PostSave<'_, '_>,
) -> BoxFuture<'a, Result<(), Refused>> {
    Box::pin(async move {
        let reference = uploaded.reference.as_uuid();
        let row = sqlx::query("SELECT id FROM sample_doc WHERE blob_ref = $1")
            .bind(reference)
            .fetch_optional(ps.connection())
            .await
            .expect("the post-upload policy reads its referencing doc");
        if let Some(row) = row {
            let id: Uuid = row.get("id");
            ps.impact_caused::<Doc, _>(&id, "attachment_uploaded")
                .expect("the post-upload policy impacts the doc view");
            ps.emit(AttachmentUploaded { reference })
                .expect("the post-upload policy emits its integration event");
        }
        Ok(())
    })
}

pub fn refuse_upload<'a>(
    _uploaded: Uploaded<'a>,
    ps: &'a mut PostSave<'_, '_>,
) -> BoxFuture<'a, Result<(), Refused>> {
    Box::pin(async move { Err(ps.refuse(UPLOAD_REFUSED)) })
}

#[derive(Debug, Serialize)]
pub struct AttachmentUploaded {
    pub reference: Uuid,
}

impl OutboundEvent for AttachmentUploaded {
    fn coords(&self) -> EventCoords {
        EventCoords {
            producer: Bc::new("sample").unwrap(),
            aggregate: Aggregate::new("attachment").unwrap(),
            fact: PastFact::new("uploaded").unwrap(),
            version: 1,
        }
    }

    fn event_id(&self) -> Uuid {
        Uuid::now_v7()
    }
}
