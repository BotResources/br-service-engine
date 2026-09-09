use example_contract::{
    ReplyCancelled, ReplyFinished, reply_cancelled_coords, reply_finished_coords,
};
use br_core_integration::{Aggregate, Bc, CommandCoords, Verb};
use serde::{Deserialize, Serialize};
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use uuid::Uuid;

pub struct InboundReplyFinished(pub ReplyFinished);

impl ReactionMessage for InboundReplyFinished {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(reply_finished_coords())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload).map(InboundReplyFinished)
    }
}

pub struct InboundReplyCancelled(pub ReplyCancelled);

impl ReactionMessage for InboundReplyCancelled {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(reply_cancelled_coords())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload).map(InboundReplyCancelled)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelTimedOut {
    pub reply_id: Uuid,
    pub board_id: Uuid,
}

fn cancel_timeout_coords() -> CommandCoords {
    CommandCoords {
        receiver: Bc::new(example_contract::SERVICE).expect("service bc"),
        aggregate: Aggregate::new("reply").expect("reply aggregate"),
        verb: Verb::new("cancel-timeout").expect("cancel-timeout verb"),
        version: 1,
    }
}

impl ReactionMessage for CancelTimedOut {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(cancel_timeout_coords())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}
