use example_contract::{ReplyFinished, reply_finished_coords};
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};

pub struct InboundReplyFinished(pub ReplyFinished);

impl ReactionMessage for InboundReplyFinished {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(reply_finished_coords())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload).map(InboundReplyFinished)
    }
}
