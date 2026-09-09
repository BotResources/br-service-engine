use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::UploadUrl;
use service_engine::pipeline::{Mutation, MutationInput, OneShot};
use uuid::Uuid;

use super::aggregate::{Reply, ReplyRow};
use super::blob::Attachment;
use super::presence::{Typing, TypingKey, TypingValue};
use crate::kernel::{AppFault, AppPrincipal};

#[derive(Debug, Deserialize)]
pub struct StartReply {
    pub id: Uuid,
    pub board_id: Uuid,
}

impl MutationInput for StartReply {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "start_reply";
}

pub fn start_reply<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: StartReply,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        let reply = ReplyRow {
            id: input.id,
            board_id: input.board_id,
            text: String::new(),
            status: "streaming".to_string(),
            blob_ref: None,
        };
        cx.create(&reply).await?;
        cx.impact_caused::<Reply, _>(&reply.id, "started")?;
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct SetTyping {
    pub board: Uuid,
    pub label: String,
}

impl MutationInput for SetTyping {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "set_typing";
}

pub fn set_typing<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: SetTyping,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        let user = cx.principal().user();
        cx.present::<Typing>(
            &TypingKey {
                board: input.board,
                user,
            },
            &TypingValue { label: input.label },
        );
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct AttachReply {
    pub reply_id: Uuid,
    pub name: String,
    pub content_type: String,
}

impl MutationInput for AttachReply {
    type Output = OneShot<UploadUrl>;
    type Error = AppFault;
    const NAME: &'static str = "attach_reply";
}

pub fn attach_reply<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: AttachReply,
) -> BoxFuture<'m, Result<OneShot<UploadUrl>, AppFault>> {
    Box::pin(async move {
        let blob = cx.blob::<Attachment>(input.name, input.content_type)?;
        let mut reply = cx
            .load::<ReplyRow>(&input.reply_id)
            .await?
            .ok_or(AppFault::NotFound)?;
        reply.blob_ref = Some(blob.reference().as_uuid());
        cx.save(&reply).await?;
        cx.impact_caused::<Reply, _>(&reply.id, "attached")?;
        Ok(OneShot(blob.upload_url()))
    })
}
