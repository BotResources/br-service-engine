use futures_util::future::BoxFuture;
use service_engine::Blobs;
use service_engine::blobs::Uploaded;
use service_engine::pipeline::{PostSave, Refused};
use sqlx::Row;
use uuid::Uuid;

use super::aggregate::{Reply, ReplyCause};

pub struct Attachment;

impl Blobs for Attachment {
    const KIND: &'static str = "reply_attachment";
}

pub fn impact_reply_on_upload<'a>(
    uploaded: Uploaded<'a>,
    ps: &'a mut PostSave<'_, '_>,
) -> BoxFuture<'a, Result<(), Refused>> {
    Box::pin(async move {
        let reference = uploaded.reference.as_uuid();
        let row = sqlx::query("SELECT id FROM reply WHERE blob_ref = $1")
            .bind(reference)
            .fetch_optional(ps.connection())
            .await
            .expect("the post-upload policy reads its referencing reply");
        if let Some(row) = row {
            let id: Uuid = row.get("id");
            ps.impact_caused::<Reply, _>(&id, ReplyCause::AttachmentUploaded)
                .expect("the post-upload policy impacts the reply view");
        }
        Ok(())
    })
}
