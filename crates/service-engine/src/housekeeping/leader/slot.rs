use crate::advisory;
use crate::name::{JobName, PodId, RelayName};
use crate::time::Timestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlotKind {
    Relay,
    Cron,
}

impl SlotKind {
    pub const fn prefix(self) -> &'static str {
        match self {
            Self::Relay => "relay",
            Self::Cron => "cron",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotName {
    Relay(RelayName),
    Cron(JobName),
}

impl SlotName {
    pub const fn kind(&self) -> SlotKind {
        match self {
            Self::Relay(_) => SlotKind::Relay,
            Self::Cron(_) => SlotKind::Cron,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Relay(name) => name.as_str(),
            Self::Cron(name) => name.as_str(),
        }
    }

    pub fn qualified(&self) -> String {
        format!("{}:{}", self.kind().prefix(), self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub(super) name: SlotName,
    pub(super) slot: Timestamp,
    pub(super) pod: PodId,
    pub(super) lease_until: Timestamp,
}

impl Lease {
    pub const fn kind(&self) -> SlotKind {
        self.name.kind()
    }

    pub const fn name(&self) -> &SlotName {
        &self.name
    }

    pub fn qualified_name(&self) -> String {
        self.name.qualified()
    }

    pub const fn slot(&self) -> Timestamp {
        self.slot
    }

    pub const fn pod(&self) -> &PodId {
        &self.pod
    }

    pub const fn lease_until(&self) -> Timestamp {
        self.lease_until
    }
}

pub fn advisory_key(name: &SlotName) -> i64 {
    advisory::lock_id(
        advisory::LEADER_SLOT,
        &[name.kind().prefix().as_bytes(), name.as_str().as_bytes()],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn relay(name: &'static str) -> SlotName {
        SlotName::Relay(RelayName::from_static(name))
    }

    fn cron(name: &'static str) -> SlotName {
        SlotName::Cron(JobName::from_static(name))
    }

    #[test]
    fn a_slot_row_carries_the_kind_that_claimed_it() {
        assert_eq!(relay("nightly").qualified(), "relay:nightly");
        assert_eq!(cron("nightly").qualified(), "cron:nightly");
        assert_ne!(
            relay("nightly").qualified(),
            cron("nightly").qualified(),
            "a relay and a job of the same name would otherwise share one (name, slot) row, and \
             one of the two would be silently skipped for that slot"
        );
    }

    #[test]
    fn the_advisory_key_is_stable_and_separates_kinds_and_names() {
        assert_eq!(
            advisory_key(&relay("kv_drain")),
            advisory_key(&relay("kv_drain"))
        );
        assert_ne!(
            advisory_key(&relay("kv_drain")),
            advisory_key(&relay("kv_drainn"))
        );
        assert_ne!(
            advisory_key(&relay("nightly")),
            advisory_key(&cron("nightly"))
        );
        assert_eq!(advisory_key(&relay("kv_drain")), 217_170_732_172_311_147);
    }
}
