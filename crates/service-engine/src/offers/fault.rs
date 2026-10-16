use std::sync::{Arc, OnceLock};

use crate::error::RelayError;
use crate::inbound::DeadLetters;
use crate::name::RelayName;
use crate::nats::{KvKey, Slot};

#[derive(Clone, Default)]
pub(crate) struct DeadLetterSlot(Arc<OnceLock<DeadLetters>>);

impl DeadLetterSlot {
    pub(crate) fn bind(&self, dead_letters: DeadLetters) {
        let _ = self.0.set(dead_letters);
    }

    fn get(&self, offer: &RelayName) -> Result<&DeadLetters, RelayError> {
        self.0.get().ok_or_else(|| {
            RelayError::Publish(format!(
                "offer {offer} met an undecodable published value before the engine bound its dead letters"
            ))
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Isolation<'a> {
    dead_letters: &'a DeadLetterSlot,
    offer: &'a RelayName,
}

impl<'a> Isolation<'a> {
    pub(crate) fn new(dead_letters: &'a DeadLetterSlot, offer: &'a RelayName) -> Self {
        Self {
            dead_letters,
            offer,
        }
    }

    pub(crate) fn offer(&self) -> &'a RelayName {
        self.offer
    }

    pub(crate) async fn isolate<V>(
        &self,
        key: &KvKey,
        slot: &Slot<V>,
        what: &str,
    ) -> Result<(), RelayError> {
        let Slot::Undecodable(fault) = slot else {
            return Ok(());
        };
        let reason = fault.reason_for(what);
        tracing::warn!(
            offer = %self.offer,
            key = key.as_str(),
            reason = %reason,
            "an undecodable value sits under the offer prefix; it is dead-lettered and overwritten or retracted",
        );
        self.dead_letters
            .get(self.offer)?
            .record_published_value(self.offer.as_str(), key.as_str(), &reason)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offer() -> RelayName {
        RelayName::new("widget_offer").unwrap()
    }

    fn key() -> KvKey {
        KvKey::new("sample/widgets/one").unwrap()
    }

    #[tokio::test]
    async fn an_undecodable_value_met_before_the_dead_letters_are_bound_fails_closed() {
        let slot = DeadLetterSlot::default();
        let offer = offer();
        let undecodable = Slot::<u32>::from_json(b"not json");
        let outcome = Isolation::new(&slot, &offer)
            .isolate(&key(), &undecodable, "the value")
            .await;
        assert!(matches!(outcome, Err(RelayError::Publish(_))));
    }

    #[tokio::test]
    async fn a_decodable_value_needs_no_dead_letters() {
        let slot = DeadLetterSlot::default();
        let offer = offer();
        let decoded = Slot::<u32>::from_json(b"7");
        let outcome = Isolation::new(&slot, &offer)
            .isolate(&key(), &decoded, "the value")
            .await;
        assert!(outcome.is_ok());
    }
}
