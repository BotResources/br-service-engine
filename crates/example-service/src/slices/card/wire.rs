use br_core_integration::{Aggregate, Bc, CommandCoords, EventCoords, Verb};
use example_contract::{
    CardReady, CreateCard, PersonCreated, SERVICE, card_ready_coords, create_card_coords,
    person_created_coords,
};
use serde::{Deserialize, Serialize};
use service_engine::inbound::{ReactionCoordinates, ReactionMessage};
use service_engine::pipeline::OutboundEvent;
use uuid::Uuid;

pub struct InboundCreateCard(pub CreateCard);

impl ReactionMessage for InboundCreateCard {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(create_card_coords())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload).map(InboundCreateCard)
    }
}

pub struct InboundPersonCreated(pub PersonCreated);

impl ReactionMessage for InboundPersonCreated {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Event(person_created_coords())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload).map(InboundPersonCreated)
    }
}

pub struct OutCardReady(pub CardReady);

impl Serialize for OutCardReady {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl OutboundEvent for OutCardReady {
    fn coords(&self) -> EventCoords {
        card_ready_coords()
    }

    fn event_id(&self) -> Uuid {
        self.0.card_id
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CardDeadline {
    pub card_id: Uuid,
}

pub fn card_deadline_coords() -> CommandCoords {
    CommandCoords {
        receiver: Bc::new(SERVICE).expect("service bc"),
        aggregate: Aggregate::new("card").expect("card aggregate"),
        verb: Verb::new("deadline").expect("deadline verb"),
        version: 1,
    }
}

impl ReactionMessage for CardDeadline {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(card_deadline_coords())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}
