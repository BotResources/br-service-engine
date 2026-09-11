use std::time::Duration;

use br_core_integration::{CommandCoords, EventCoords};

use crate::inbound::budget::{Budgets, DEFAULT_BACKOFF_BASE, DEFAULT_BACKOFF_MAX};
use crate::inbound::message::Source;
use crate::nats::{INTEGRATION_CMD, INTEGRATION_EVT, command_subject, event_subject};

#[derive(Debug, Clone)]
pub enum ReactionCoordinates {
    Command(CommandCoords),
    Event(EventCoords),
}

impl ReactionCoordinates {
    pub fn source(&self) -> Source {
        match self {
            Self::Command(_) => Source::Command,
            Self::Event(_) => Source::Event,
        }
    }

    pub fn stream(&self) -> &'static str {
        match self {
            Self::Command(_) => INTEGRATION_CMD,
            Self::Event(_) => INTEGRATION_EVT,
        }
    }

    pub fn subject(&self) -> String {
        match self {
            Self::Command(coords) => command_subject(coords),
            Self::Event(coords) => event_subject(coords),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Subscription {
    pub reaction: String,
    pub coordinates: ReactionCoordinates,
    pub budgets: Budgets,
}

impl Subscription {
    pub fn new(reaction: impl Into<String>, coordinates: ReactionCoordinates) -> Self {
        Self {
            reaction: reaction.into(),
            coordinates,
            budgets: Budgets::default(),
        }
    }

    pub fn with_budgets(mut self, budgets: Budgets) -> Self {
        self.budgets = budgets;
        self
    }
}

#[derive(Debug, Clone)]
pub struct InboundConfig {
    pub ack_wait: Duration,
    pub max_ack_pending: i64,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
}

pub const DEFAULT_ACK_WAIT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_ACK_PENDING: i64 = 256;

impl Default for InboundConfig {
    fn default() -> Self {
        Self {
            ack_wait: DEFAULT_ACK_WAIT,
            max_ack_pending: DEFAULT_MAX_ACK_PENDING,
            backoff_base: DEFAULT_BACKOFF_BASE,
            backoff_max: DEFAULT_BACKOFF_MAX,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use br_core_integration::{Aggregate, Bc, PastFact, Verb};

    #[test]
    fn command_coordinates_render_the_command_stream_and_subject() {
        let coords = ReactionCoordinates::Command(CommandCoords {
            receiver: Bc::new("jobs").unwrap(),
            aggregate: Aggregate::new("job").unwrap(),
            verb: Verb::new("create").unwrap(),
            version: 1,
        });
        assert_eq!(coords.stream(), INTEGRATION_CMD);
        assert_eq!(coords.subject(), "integration.cmd.jobs.job.create.v1");
        assert_eq!(coords.source(), Source::Command);
    }

    #[test]
    fn event_coordinates_render_the_event_stream_and_subject() {
        let coords = ReactionCoordinates::Event(EventCoords {
            producer: Bc::new("orders").unwrap(),
            aggregate: Aggregate::new("order").unwrap(),
            fact: PastFact::new("confirmed").unwrap(),
            version: 2,
        });
        assert_eq!(coords.stream(), INTEGRATION_EVT);
        assert_eq!(
            coords.subject(),
            "integration.evt.orders.order.confirmed.v2"
        );
        assert_eq!(coords.source(), Source::Event);
    }
}
