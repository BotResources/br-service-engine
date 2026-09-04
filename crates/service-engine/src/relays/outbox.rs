use std::sync::Mutex;

use br_util_nats_fabric::{OutboxRelay, RelayHealthReceiver, RelayPass};
use futures_util::future::BoxFuture;
use sqlx::{PgConnection, PgPool};

use crate::error::RelayError;
use crate::name::RelayName;
use crate::relay::{Claim, Discipline, Drained, Relay};

pub struct FabricOutboxRelay {
    name: RelayName,
    hosted: OutboxRelay,
    cap: usize,
    last: Mutex<Option<RelayPass>>,
}

impl FabricOutboxRelay {
    pub fn hosting(name: RelayName, hosted: OutboxRelay, cap: usize) -> Self {
        Self {
            name,
            hosted,
            cap: cap.max(1),
            last: Mutex::new(None),
        }
    }

    pub fn health(&self) -> RelayHealthReceiver {
        self.hosted.health()
    }

    pub fn last_pass(&self) -> Option<RelayPass> {
        *self
            .last
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn record(&self, pass: RelayPass) {
        *self
            .last
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(pass);
    }

    async fn run(&self) -> Result<Drained, RelayError> {
        let pass = self
            .hosted
            .run_once_detailed()
            .await
            .map_err(|error| RelayError::Relay(Box::new(error)))?;
        self.record(pass);
        verdict(&self.name, pass.picked, pass.structural, self.cap)
    }
}

fn verdict(
    name: &RelayName,
    picked: usize,
    structural: usize,
    cap: usize,
) -> Result<Drained, RelayError> {
    if structural > 0 {
        return Err(RelayError::Publish(format!(
            "{name} left {structural} row(s) pending on a structural failure"
        )));
    }
    Ok(Drained::rows(picked, picked >= cap))
}

impl Relay for FabricOutboxRelay {
    fn name(&self) -> RelayName {
        self.name.clone()
    }

    fn discipline(&self) -> Discipline {
        Discipline::RowClaim
    }

    fn drain<'a>(
        &'a self,
        _conn: &'a mut PgConnection,
        _claim: &'a Claim,
    ) -> BoxFuture<'a, Result<Drained, RelayError>> {
        Box::pin(self.run())
    }

    fn hosted_drain<'a>(
        &'a self,
        _pg: &'a PgPool,
        _claim: &'a Claim,
    ) -> Option<BoxFuture<'a, Result<Drained, RelayError>>> {
        Some(Box::pin(self.run()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name() -> RelayName {
        RelayName::from_static("integration_outbox")
    }

    #[test]
    fn a_pass_that_filled_its_cap_asks_the_beat_to_come_back_before_the_next_one() {
        assert_eq!(verdict(&name(), 4, 0, 4).unwrap(), Drained::rows(4, true));
        assert_eq!(verdict(&name(), 1, 0, 4).unwrap(), Drained::rows(1, false));
        assert_eq!(verdict(&name(), 0, 0, 4).unwrap(), Drained::NOTHING);
    }

    #[test]
    fn a_structural_failure_backs_the_relay_off_instead_of_reporting_a_clean_pass() {
        let error = verdict(&name(), 3, 1, 256).unwrap_err();
        assert!(matches!(error, RelayError::Publish(_)));
        assert!(error.to_string().contains("integration_outbox"));
    }
}
