use async_graphql::{Context, Json, Object, Result, Subscription};
use futures_util::{Stream, StreamExt};
use serde::Serialize;
use service_engine::graphql::SliceFragment;
use service_engine::session::{WindowParams, WindowSpec};
use service_engine::{JsonScalar, MutationAck, Query};
use uuid::Uuid;

use super::mutations::{AttachReply, SetTyping, StartReply};
use super::presence::{Typing, TypingView};
use super::view::{RepliesView, ReplyView};
use crate::kernel::AppPrincipal;

service_engine::subscription_union! {
    view = ReplyViewUnion;
    delta = ReplyDelta { reset = ReplyReset, upsert = ReplyUpsert, remove = ReplyRemove };
    Reply => RepliesView => ReplyView,
}

service_engine::presence_subscription_union! {
    view = TypingUnion;
    delta = TypingDelta { reset = TypingReset, upsert = TypingUpsert, remove = TypingRemove };
    Typing => Typing => TypingView,
}

pub const FRAGMENT: SliceFragment = SliceFragment {
    slice: "reply",
    root_fields: &[
        "reply",
        "replyDeltas",
        "typingDeltas",
        "startReply",
        "setTyping",
        "attachReply",
    ],
    types: &["ReplyView"],
};

#[derive(Serialize)]
struct TypingWindow {
    board: Uuid,
}

#[derive(Default)]
pub struct ReplyQuery;

#[Object]
impl ReplyQuery {
    async fn reply(&self, ctx: &Context<'_>, id: Uuid) -> Result<Option<ReplyView>> {
        Query::<AppPrincipal>::new(ctx)?
            .fetch::<RepliesView>(&id)
            .await
    }
}

#[derive(Default)]
pub struct ReplyMutation;

#[Object]
impl ReplyMutation {
    async fn start_reply(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
        board_id: Uuid,
    ) -> Result<MutationAck> {
        service_engine::ack::<AppPrincipal, StartReply>(ctx, StartReply { id, board_id }).await
    }

    async fn set_typing(
        &self,
        ctx: &Context<'_>,
        board: Uuid,
        label: String,
    ) -> Result<MutationAck> {
        service_engine::ack::<AppPrincipal, SetTyping>(ctx, SetTyping { board, label }).await
    }

    async fn attach_reply(
        &self,
        ctx: &Context<'_>,
        reply_id: Uuid,
        name: String,
        content_type: String,
    ) -> Result<JsonScalar> {
        let url = service_engine::execute::<AppPrincipal, AttachReply>(
            ctx,
            AttachReply {
                reply_id,
                name,
                content_type,
            },
        )
        .await?;
        let (endpoint, fields) = url.into_inner().into_parts();
        let fields: serde_json::Map<String, serde_json::Value> = fields
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::String(v)))
            .collect();
        Ok(Json(
            serde_json::json!({ "endpoint": endpoint, "fields": fields }),
        ))
    }
}

#[derive(Default)]
pub struct ReplySubscription;

#[Subscription]
impl ReplySubscription {
    async fn reply_deltas(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = Result<ReplyDelta>>> {
        let stream = service_engine::attach::<AppPrincipal>(
            ctx,
            vec![WindowSpec::new(
                RepliesView::NAME,
                WindowParams::none(),
                false,
            )],
        )
        .await?;
        Ok(stream.map(|delta| ReplyDelta::from_delta(&delta)))
    }

    async fn typing_deltas(
        &self,
        ctx: &Context<'_>,
        board: Uuid,
    ) -> Result<impl Stream<Item = Result<TypingDelta>>> {
        let params = WindowParams::encode(&TypingWindow { board })?;
        let stream = service_engine::attach::<AppPrincipal>(
            ctx,
            vec![WindowSpec::new(Typing::NAME, params, false)],
        )
        .await?;
        Ok(stream.map(|delta| TypingDelta::from_delta(&delta)))
    }
}
