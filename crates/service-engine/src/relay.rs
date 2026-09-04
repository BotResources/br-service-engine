use futures_util::future::BoxFuture;
use sqlx::{PgConnection, PgPool};

use crate::error::RelayError;
use crate::name::{PodId, RelayName};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Discipline {
    RowClaim,
    Leader,
}

#[derive(Debug, Clone)]
pub struct Claim {
    pod: PodId,
    batch: usize,
}

impl Claim {
    pub const FOR_UPDATE_SKIP_LOCKED: &'static str = "FOR UPDATE SKIP LOCKED";

    pub fn new(pod: PodId, batch: usize) -> Self {
        Self { pod, batch }
    }

    pub const fn for_update_skip_locked(&self) -> &'static str {
        Self::FOR_UPDATE_SKIP_LOCKED
    }

    pub fn pod(&self) -> &PodId {
        &self.pod
    }

    pub const fn batch(&self) -> usize {
        self.batch
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Drained {
    pub rows: usize,
    pub more: bool,
}

impl Drained {
    pub const NOTHING: Self = Self {
        rows: 0,
        more: false,
    };

    pub const fn rows(rows: usize, more: bool) -> Self {
        Self { rows, more }
    }
}

pub trait Relay: Send + Sync + 'static {
    fn name(&self) -> RelayName;

    fn drain<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        claim: &'a Claim,
    ) -> BoxFuture<'a, Result<Drained, RelayError>>;

    fn discipline(&self) -> Discipline {
        Discipline::RowClaim
    }

    fn hosted_drain<'a>(
        &'a self,
        _pg: &'a PgPool,
        _claim: &'a Claim,
    ) -> Option<BoxFuture<'a, Result<Drained, RelayError>>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_claim_relay_is_handed_the_engines_own_locking_fragment() {
        let claim = Claim::new(PodId::new("svc-sample-0").unwrap(), 64);
        assert_eq!(claim.for_update_skip_locked(), "FOR UPDATE SKIP LOCKED");
        assert_eq!(claim.batch(), 64);
        assert_eq!(claim.pod().as_str(), "svc-sample-0");
    }
}
