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
