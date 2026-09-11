use br_core_integration::{Aggregate, Bc, CommandCoords, EventCoords, PastFact, Verb};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{SERVICE, TWIN};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCard {
    pub card_id: Uuid,
    pub board_id: Uuid,
    pub title: String,
}

pub fn create_card_coords() -> CommandCoords {
    CommandCoords {
        receiver: Bc::new(SERVICE).expect("service bc"),
        aggregate: Aggregate::new("card").expect("card aggregate"),
        verb: Verb::new("create").expect("create verb"),
        version: 1,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CardReady {
    pub card_id: Uuid,
    pub board_id: Uuid,
}

pub fn card_ready_coords() -> EventCoords {
    EventCoords {
        producer: Bc::new(SERVICE).expect("service bc"),
        aggregate: Aggregate::new("card").expect("card aggregate"),
        fact: PastFact::new("ready").expect("ready fact"),
        version: 1,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyFinished {
    pub reply_id: Uuid,
    pub board_id: Uuid,
    pub last_seq: u64,
    pub hash: String,
}

pub fn reply_finished_coords() -> CommandCoords {
    CommandCoords {
        receiver: Bc::new(SERVICE).expect("service bc"),
        aggregate: Aggregate::new("reply").expect("reply aggregate"),
        verb: Verb::new("finish").expect("finish verb"),
        version: 1,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyCancelled {
    pub reply_id: Uuid,
    pub board_id: Uuid,
    pub last_seq: u64,
    pub hash: String,
}

pub fn reply_cancelled_coords() -> CommandCoords {
    CommandCoords {
        receiver: Bc::new(SERVICE).expect("service bc"),
        aggregate: Aggregate::new("reply").expect("reply aggregate"),
        verb: Verb::new("cancel").expect("cancel verb"),
        version: 1,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SealFailed {
    pub reply_id: Uuid,
    pub board_id: Uuid,
    pub reason: String,
}

pub fn seal_failed_coords() -> EventCoords {
    EventCoords {
        producer: Bc::new(SERVICE).expect("service bc"),
        aggregate: Aggregate::new("reply").expect("reply aggregate"),
        fact: PastFact::new("seal-failed").expect("seal-failed fact"),
        version: 1,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonCreated {
    pub person_id: Uuid,
    pub board_id: Uuid,
    pub display_name: String,
}

pub fn person_created_coords() -> EventCoords {
    EventCoords {
        producer: Bc::new(TWIN).expect("twin bc"),
        aggregate: Aggregate::new("person").expect("person aggregate"),
        fact: PastFact::new("created").expect("created fact"),
        version: 1,
    }
}
