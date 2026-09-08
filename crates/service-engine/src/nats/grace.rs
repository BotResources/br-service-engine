use std::time::{Duration, Instant};

use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NatsCondition {
    Up,
    WithinGrace,
    PastGrace,
}

impl NatsCondition {
    pub fn is_up(self) -> bool {
        matches!(self, Self::Up)
    }

    pub fn holds_readiness(self) -> bool {
        matches!(self, Self::PastGrace)
    }
}

#[derive(Debug)]
pub struct NatsHealth {
    grace: Duration,
    first_unreachable: Option<Instant>,
}

impl NatsHealth {
    pub fn new(grace: Duration) -> Self {
        Self {
            grace,
            first_unreachable: None,
        }
    }

    pub fn observe(&mut self, reachable: bool, now: Instant) -> NatsCondition {
        if reachable {
            self.first_unreachable = None;
            return NatsCondition::Up;
        }
        let since = *self.first_unreachable.get_or_insert(now);
        if now.duration_since(since) >= self.grace {
            NatsCondition::PastGrace
        } else {
            NatsCondition::WithinGrace
        }
    }
}

pub type NatsHealthReceiver = watch::Receiver<NatsCondition>;

pub struct NatsHealthChannel {
    sender: watch::Sender<NatsCondition>,
    receiver: NatsHealthReceiver,
}

impl NatsHealthChannel {
    pub fn new() -> Self {
        let (sender, receiver) = watch::channel(NatsCondition::Up);
        Self { sender, receiver }
    }

    pub fn receiver(&self) -> NatsHealthReceiver {
        self.receiver.clone()
    }

    pub fn set(&self, condition: NatsCondition) {
        self.sender.send_if_modified(|current| {
            if *current == condition {
                false
            } else {
                *current = condition;
                true
            }
        });
    }
}

impl Default for NatsHealthChannel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reachable_broker_is_up_and_clears_any_prior_outage() {
        let mut health = NatsHealth::new(Duration::from_secs(10));
        let base = Instant::now();
        assert_eq!(health.observe(false, base), NatsCondition::WithinGrace);
        assert_eq!(health.observe(true, base), NatsCondition::Up);
        assert_eq!(
            health.observe(false, base + Duration::from_secs(1)),
            NatsCondition::WithinGrace,
            "a return to reachable resets the grace clock, so a later blink starts a fresh window"
        );
    }

    #[test]
    fn an_outage_stays_within_grace_until_the_window_elapses_then_holds_readiness_down() {
        let mut health = NatsHealth::new(Duration::from_secs(10));
        let base = Instant::now();
        assert_eq!(health.observe(false, base), NatsCondition::WithinGrace);
        assert_eq!(
            health.observe(false, base + Duration::from_secs(9)),
            NatsCondition::WithinGrace
        );
        assert_eq!(
            health.observe(false, base + Duration::from_secs(10)),
            NatsCondition::PastGrace
        );
        assert!(NatsCondition::PastGrace.holds_readiness());
        assert!(!NatsCondition::WithinGrace.holds_readiness());
        assert!(!NatsCondition::Up.holds_readiness());
    }

    #[test]
    fn the_channel_publishes_only_transitions() {
        let channel = NatsHealthChannel::new();
        let mut rx = channel.receiver();
        assert_eq!(*rx.borrow(), NatsCondition::Up);
        rx.mark_unchanged();
        channel.set(NatsCondition::Up);
        assert!(!rx.has_changed().unwrap());
        channel.set(NatsCondition::PastGrace);
        assert!(rx.has_changed().unwrap());
        assert_eq!(*rx.borrow_and_update(), NatsCondition::PastGrace);
    }
}
