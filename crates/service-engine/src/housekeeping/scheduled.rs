use std::sync::Arc;

use crate::error::EngineError;
use crate::transport::PgListenNotify;

pub const DEFAULT_SCHEDULED_BATCH: i64 = 256;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScheduledRound {
    pub fired: usize,
    pub more: bool,
}

pub struct ScheduledBoundaries {
    transport: Arc<PgListenNotify>,
    batch: i64,
}

impl ScheduledBoundaries {
    pub fn new(transport: Arc<PgListenNotify>) -> Self {
        Self {
            transport,
            batch: DEFAULT_SCHEDULED_BATCH,
        }
    }

    pub fn with_batch(mut self, batch: i64) -> Result<Self, EngineError> {
        self.batch = validate_batch(batch)?;
        Ok(self)
    }

    pub const fn batch(&self) -> i64 {
        self.batch
    }

    pub async fn fire_due(&self) -> Result<ScheduledRound, EngineError> {
        Ok(round(
            self.transport.fire_due(self.batch).await?,
            self.batch,
        ))
    }
}

impl std::fmt::Debug for ScheduledBoundaries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScheduledBoundaries")
            .field("batch", &self.batch)
            .finish_non_exhaustive()
    }
}

fn validate_batch(batch: i64) -> Result<i64, EngineError> {
    if batch <= 0 {
        return Err(EngineError::Config(
            "a scheduled-boundary batch must be positive, or the beat would claim nothing".into(),
        ));
    }
    Ok(batch)
}

fn round(fired: usize, batch: i64) -> ScheduledRound {
    ScheduledRound {
        fired,
        more: i64::try_from(fired).is_ok_and(|fired| fired >= batch),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_that_would_claim_nothing_is_refused_instead_of_never_firing_a_boundary() {
        assert!(validate_batch(0).is_err());
        assert!(validate_batch(-1).is_err());
        assert_eq!(validate_batch(1).unwrap(), 1);
    }

    #[test]
    fn a_full_claim_asks_the_beat_to_come_back_before_it_sleeps() {
        assert_eq!(round(0, 4), ScheduledRound::default());
        assert_eq!(
            round(3, 4),
            ScheduledRound {
                fired: 3,
                more: false
            }
        );
        assert_eq!(
            round(4, 4),
            ScheduledRound {
                fired: 4,
                more: true
            },
            "a boundary backlog larger than one batch must not wait a whole beat per batch"
        );
    }
}
