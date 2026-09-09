use futures_util::future::BoxFuture;
use service_engine::SealHash;
use service_engine::accumulator::ChunkSeq;
use service_engine::error::EngineError;
use service_engine::pipeline::Reaction;

use super::aggregate::{Reply, ReplyRow};
use super::stream::ReplyText;
use super::wire::InboundReplyFinished;
use crate::kernel::error::ReactionFault;

pub fn reply_finished<'r>(
    cx: &'r mut Reaction<'r>,
    msg: InboundReplyFinished,
) -> BoxFuture<'r, Result<(), ReactionFault>> {
    Box::pin(async move {
        let cmd = msg.0;
        let last_seq = ChunkSeq::new(cmd.last_seq)?;
        let hash = SealHash::from_hex(&cmd.hash)
            .map_err(|error| ReactionFault::Terminal(error.to_string()))?;
        let text: String = match cx.seal::<ReplyText>(&cmd.reply_id, last_seq, hash).await {
            Ok(text) => text,
            Err(
                error @ (EngineError::SealTruncated { .. } | EngineError::SealHashMismatch { .. }),
            ) => {
                return Err(ReactionFault::Terminal(error.to_string()));
            }
            Err(error) => return Err(error.into()),
        };
        let mut reply = cx
            .load::<ReplyRow>(&cmd.reply_id)
            .await?
            .unwrap_or_else(|| ReplyRow::open(cmd.reply_id, cmd.board_id));
        let cause = reply.complete(text);
        cx.save(&reply).await?;
        cx.impact_caused::<Reply, _>(&cmd.reply_id, cause)?;
        Ok(())
    })
}
