use br_core_integration::{Aggregate, Bc, CommandCoords, EventCoords, Verb};
use example_contract::{
    ReplyCancelled, ReplyFinished, SealFailed, reply_cancelled_coords, reply_finished_coords,
    seal_failed_coords,
};
use serde::{Deserialize, Serialize};
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::pipeline::OutboundEvent;
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

pub struct OutSealFailed(pub SealFailed);

impl Serialize for OutSealFailed {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl OutboundEvent for OutSealFailed {
    fn coords(&self) -> EventCoords {
        seal_failed_coords()
    }

    fn event_id(&self) -> Uuid {
        self.0.reply_id
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
