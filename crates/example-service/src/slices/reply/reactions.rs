use futures_util::future::BoxFuture;
use service_engine::pipeline::Reaction;

use super::aggregate::{Reply, ReplyRow};
use super::stream::ReplyText;
use super::wire::ReplyFinished;
use crate::kernel::error::ReactionFault;

pub fn reply_finished<'r>(
    cx: &'r mut Reaction<'r>,
    cmd: ReplyFinished,
) -> BoxFuture<'r, Result<(), ReactionFault>> {
    Box::pin(async move {
        let accumulated = cx.accumulated::<ReplyText>(&cmd.reply_id).await?;
        if accumulated.gap {
            return Err(ReactionFault::NotYet(
                "the reply stream still has a gap; awaiting the missing chunks".into(),
            ));
        }
        let mut reply = cx
            .load::<ReplyRow>(&cmd.reply_id)
            .await?
            .unwrap_or(ReplyRow {
                id: cmd.reply_id,
                board_id: cmd.board_id,
                text: String::new(),
                status: "streaming".to_string(),
                blob_ref: None,
            });
        reply.text = accumulated.state;
        reply.status = "complete".to_string();
        cx.save(&reply).await?;
        cx.seal::<ReplyText>(&cmd.reply_id).await?;
        cx.impact_caused::<Reply, _>(&cmd.reply_id, "sealed")?;
        Ok(())
    })
}
