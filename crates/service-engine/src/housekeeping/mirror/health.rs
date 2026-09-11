use std::collections::BTreeMap;
use std::time::Duration;

use tokio::sync::watch;

use crate::name::MirrorName;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MirrorCondition {
    Converging,
    Converged,
    Restarting {
        attempts: u32,
        retry_in: Duration,
        reason: String,
    },
}

impl MirrorCondition {
    pub const fn is_converged(&self) -> bool {
        matches!(self, Self::Converged)
    }

    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Restarting { reason, .. } => Some(reason.as_str()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MirrorsHealth(BTreeMap<MirrorName, MirrorCondition>);

impl MirrorsHealth {
    pub fn converged(&self) -> bool {
        self.0.values().all(MirrorCondition::is_converged)
    }

    pub fn condition(&self, mirror: &MirrorName) -> Option<&MirrorCondition> {
        self.0.get(mirror)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&MirrorName, &MirrorCondition)> {
        self.0.iter()
    }

    pub fn unconverged(&self) -> impl Iterator<Item = &MirrorName> {
        self.0
            .iter()
            .filter(|(_, condition)| !condition.is_converged())
            .map(|(name, _)| name)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn set(&mut self, mirror: MirrorName, condition: MirrorCondition) {
        self.0.insert(mirror, condition);
    }
}

impl FromIterator<(MirrorName, MirrorCondition)> for MirrorsHealth {
    fn from_iter<I: IntoIterator<Item = (MirrorName, MirrorCondition)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

pub type MirrorsHealthReceiver = watch::Receiver<MirrorsHealth>;

#[cfg(test)]
mod tests {
    use super::*;

    fn directory() -> MirrorName {
        MirrorName::from_static("directory")
    }

    #[test]
    fn a_service_with_no_mirror_is_converged_so_it_is_never_held_down_by_one() {
        assert!(MirrorsHealth::default().converged());
        assert!(MirrorsHealth::default().is_empty());
    }

    #[test]
    fn a_mirror_that_is_still_reconciling_holds_the_board_back() {
        let board = MirrorsHealth::from_iter([(directory(), MirrorCondition::Converging)]);
        assert!(!board.converged());
        assert_eq!(board.unconverged().collect::<Vec<_>>(), vec![&directory()]);
        assert_eq!(
            board
                .condition(&directory())
                .and_then(MirrorCondition::reason),
            None
        );
    }

    #[test]
    fn a_mirror_that_died_names_why_and_takes_the_board_out_of_converged() {
        let mut board = MirrorsHealth::from_iter([(directory(), MirrorCondition::Converged)]);
        assert!(board.converged());
        board.set(
            directory(),
            MirrorCondition::Restarting {
                attempts: 2,
                retry_in: Duration::from_millis(400),
                reason: "directory: fabric: no such bucket".to_string(),
            },
        );
        assert!(!board.converged());
        assert_eq!(
            board
                .condition(&directory())
                .and_then(MirrorCondition::reason),
            Some("directory: fabric: no such bucket")
        );
        assert_eq!(board.len(), 1);
    }
}
