use br_core_integration::{Aggregate, Bc, EventCoords, PastFact};
use serde::Serialize;
use uuid::Uuid;

use crate::erase::person::PersonId;
use crate::error::EngineError;
use crate::pipeline::OutboundEvent;

#[derive(Serialize)]
pub(crate) struct PersonErased {
    #[serde(skip)]
    producer: Bc,
    person_id: Uuid,
}

impl PersonErased {
    pub(crate) fn new(service: &str, person: PersonId) -> Result<Self, EngineError> {
        let producer = Bc::new(service).map_err(|error| {
            EngineError::Config(format!(
                "the configured service {service:?} is not a valid integration producer: {}",
                crate::chain::describe(&error)
            ))
        })?;
        Ok(Self {
            producer,
            person_id: person.as_uuid(),
        })
    }
}

impl OutboundEvent for PersonErased {
    fn coords(&self) -> EventCoords {
        EventCoords {
            producer: self.producer.clone(),
            aggregate: Aggregate::new("person").expect("`person` is a valid aggregate segment"),
            fact: PastFact::new("erased").expect("`erased` is a valid fact segment"),
            version: 1,
        }
    }

    fn event_id(&self) -> Uuid {
        self.person_id
    }
}
