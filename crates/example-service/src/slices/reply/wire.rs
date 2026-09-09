use br_core_integration::{Aggregate, Bc, CommandCoords, Verb};
use example_contract::SERVICE;
use serde::{Deserialize, Serialize};
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplyFinished {
    pub reply_id: Uuid,
    pub board_id: Uuid,
}

pub fn reply_finished_coords() -> CommandCoords {
    CommandCoords {
        receiver: Bc::new(SERVICE).expect("service bc"),
        aggregate: Aggregate::new("reply").expect("reply aggregate"),
        verb: Verb::new("finish").expect("finish verb"),
        version: 1,
    }
}

impl ReactionMessage for ReplyFinished {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(reply_finished_coords())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}
