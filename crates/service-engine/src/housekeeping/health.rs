use std::collections::BTreeMap;
use std::time::Duration;

use tokio::sync::watch;

use crate::name::RelayName;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RelayCondition {
    Healthy,
    BackingOff {
        attempts: u32,
        retry_in: Duration,
        reason: String,
    },
}

impl RelayCondition {
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Healthy => None,
            Self::BackingOff { reason, .. } => Some(reason.as_str()),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelaysHealth(BTreeMap<RelayName, RelayCondition>);

impl RelaysHealth {
    pub fn is_healthy(&self) -> bool {
        self.0.values().all(|c| *c == RelayCondition::Healthy)
    }

    pub fn condition(&self, relay: &RelayName) -> Option<&RelayCondition> {
        self.0.get(relay)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&RelayName, &RelayCondition)> {
        self.0.iter()
    }

    pub fn degraded(&self) -> impl Iterator<Item = &RelayName> {
        self.0
            .iter()
            .filter(|(_, condition)| **condition != RelayCondition::Healthy)
            .map(|(name, _)| name)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromIterator<(RelayName, RelayCondition)> for RelaysHealth {
    fn from_iter<I: IntoIterator<Item = (RelayName, RelayCondition)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

pub type RelaysHealthReceiver = watch::Receiver<RelaysHealth>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_board_holding_one_backing_off_relay_is_not_healthy() {
        let board = RelaysHealth::from_iter([
            (RelayName::from_static("outbox"), RelayCondition::Healthy),
            (
                RelayName::from_static("kv"),
                RelayCondition::BackingOff {
                    attempts: 3,
                    retry_in: Duration::from_secs(2),
                    reason: "database: connection closed".to_string(),
                },
            ),
        ]);
        assert!(!board.is_healthy());
        assert_eq!(
            board.degraded().collect::<Vec<_>>(),
            vec![&RelayName::from_static("kv")]
        );
        assert_eq!(board.len(), 2);
        assert_eq!(
            board
                .condition(&RelayName::from_static("kv"))
                .and_then(RelayCondition::reason),
            Some("database: connection closed"),
            "a degraded relay names why, so readiness can say more than DOWN"
        );
        assert_eq!(
            board
                .condition(&RelayName::from_static("outbox"))
                .and_then(RelayCondition::reason),
            None
        );
    }

    #[test]
    fn an_empty_board_is_healthy_so_a_service_with_no_relay_is_never_held_down() {
        assert!(RelaysHealth::default().is_healthy());
        assert!(RelaysHealth::default().is_empty());
    }
}
