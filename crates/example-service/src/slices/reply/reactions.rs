use example_contract::SealFailed;
use futures_util::future::BoxFuture;
use service_engine::SealHash;
use service_engine::accumulator::ChunkSeq;
use service_engine::error::EngineError;
use service_engine::pipeline::Reaction;

use super::aggregate::{Reply, ReplyRow};
use super::stream::ReplyText;
use super::wire::{CancelTimedOut, InboundReplyCancelled, InboundReplyFinished, OutSealFailed};
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
            Err(error @ EngineError::SealHashMismatch { .. }) => {
                return Err(ReactionFault::Terminal(error.to_string()));
            }
            Err(error @ EngineError::SealChunkBeyondLastSeq { .. }) => {
                return answer_seal_failed(cx, cmd.reply_id, cmd.board_id, error);
            }
            Err(error) => return Err(error.into()),
        };
        let mut reply = cx
            .load::<ReplyRow>(&cmd.reply_id)
            .await?
            .unwrap_or_else(|| ReplyRow::open(cmd.reply_id, cmd.board_id, uuid::Uuid::nil()));
        let cause = reply.complete(text);
        cx.save(&reply).await?;
        cx.impact_caused::<Reply, _>(&cmd.reply_id, cause)?;
        Ok(())
    })
}

pub fn reply_cancelled<'r>(
    cx: &'r mut Reaction<'r>,
    msg: InboundReplyCancelled,
) -> BoxFuture<'r, Result<(), ReactionFault>> {
    Box::pin(async move {
        let cmd = msg.0;
        let last_seq = ChunkSeq::new(cmd.last_seq)?;
        let hash = SealHash::from_hex(&cmd.hash)
            .map_err(|error| ReactionFault::Terminal(error.to_string()))?;
        let text: String = match cx
            .seal_partial::<ReplyText>(&cmd.reply_id, last_seq, hash)
            .await
        {
            Ok(text) => text,
            Err(error @ EngineError::SealHashMismatch { .. }) => {
                return Err(ReactionFault::Terminal(error.to_string()));
            }
            Err(error @ EngineError::SealChunkBeyondLastSeq { .. }) => {
                return answer_seal_failed(cx, cmd.reply_id, cmd.board_id, error);
            }
            Err(error) => return Err(error.into()),
        };
        let mut reply = cx
            .load::<ReplyRow>(&cmd.reply_id)
            .await?
            .unwrap_or_else(|| ReplyRow::open(cmd.reply_id, cmd.board_id, uuid::Uuid::nil()));
        let cause = reply.complete_cancelled(text);
        cx.save(&reply).await?;
        cx.impact_caused::<Reply, _>(&cmd.reply_id, cause)?;
        Ok(())
    })
}

pub fn cancel_timed_out<'r>(
    cx: &'r mut Reaction<'r>,
    msg: CancelTimedOut,
) -> BoxFuture<'r, Result<(), ReactionFault>> {
    Box::pin(async move {
        let mut reply = match cx.load::<ReplyRow>(&msg.reply_id).await? {
            Some(reply) if reply.is_open() => reply,
            _ => return Ok(()),
        };
        let text: String = match cx.seal_current::<ReplyText>(&msg.reply_id).await {
            Ok(text) => text,
            Err(EngineError::AlreadySealed { .. }) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let cause = reply.complete_cancelled(text);
        cx.save(&reply).await?;
        cx.impact_caused::<Reply, _>(&msg.reply_id, cause)?;
        Ok(())
    })
}

fn answer_seal_failed(
    cx: &mut Reaction<'_>,
    reply_id: uuid::Uuid,
    board_id: uuid::Uuid,
    error: EngineError,
) -> Result<(), ReactionFault> {
    cx.emit(OutSealFailed(SealFailed {
        reply_id,
        board_id,
        reason: error.to_string(),
    }))?;
    Ok(())
}
