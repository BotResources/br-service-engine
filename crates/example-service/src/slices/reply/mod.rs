mod aggregate;
mod blob;
pub mod graphql;
mod mutations;
mod presence;
mod reactions;
mod stream;
#[cfg(test)]
mod tests;
mod view;
mod wire;

use std::time::Duration;

use service_engine::error::EngineError;
use service_engine::{BlobPolicy, Engine};

use crate::kernel::AppPrincipal;

pub use stream::ReplyText;

pub fn register(engine: &mut Engine<AppPrincipal>) -> Result<(), EngineError> {
    engine.register_accumulator(stream::ReplyText)?;
    engine.register_presence::<presence::Typing>(Duration::from_secs(5))?;
    engine.register_presence::<presence::CancelSignal>(Duration::from_secs(60))?;
    engine.register_view(view::RepliesView)?;
    engine.register_reaction::<wire::InboundReplyFinished, _, _>(
        "reply-finished",
        reactions::reply_finished,
    )?;
    engine.register_reaction::<wire::InboundReplyCancelled, _, _>(
        "reply-cancelled",
        reactions::reply_cancelled,
    )?;
    engine.register_reaction::<wire::CancelTimedOut, _, _>(
        "reply-cancel-timeout",
        reactions::cancel_timed_out,
    )?;
    engine.register_mutation::<mutations::StartReply, _>(mutations::start_reply)?;
    engine.register_mutation::<mutations::SetTyping, _>(mutations::set_typing)?;
    engine.register_mutation::<mutations::CancelReply, _>(mutations::cancel_reply)?;
    engine.register_mutation::<mutations::AttachReply, _>(mutations::attach_reply)?;
    if engine.blobs_configured() {
        engine.register_blobs::<blob::Attachment>(BlobPolicy {
            max_bytes: 50 << 20,
            orphan_after: Duration::from_secs(24 * 60 * 60),
        })?;
        engine.register_post_upload_policy::<blob::Attachment, _>(blob::impact_reply_on_upload)?;
        engine.require_post_upload_policy::<blob::Attachment>()?;
    }
    engine.register_schema_slice(service_engine::graphql::SliceFragment::derive::<
        graphql::ReplyQuery,
        graphql::ReplyMutation,
        graphql::ReplySubscription,
    >("reply"))?;
    Ok(())
}
