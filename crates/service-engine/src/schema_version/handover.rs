use std::time::Duration;

use sqlx::PgPool;
use tokio::time::Instant;

use crate::config::EngineConfig;
use crate::error::EngineError;
use crate::readiness::ReadinessHandle;
use crate::schema::ENGINE_SCHEMA_VERSION;

use super::{claim_schema_version, handover_wait_reason};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandoverStep {
    Recheck(Duration),
    Refuse,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HandoverDeadline {
    deadline: Instant,
    beat: Duration,
}

impl HandoverDeadline {
    pub(crate) fn from_first_conflict(
        first_conflict: Instant,
        liveness: Duration,
        beat: Duration,
    ) -> Self {
        Self {
            deadline: first_conflict + liveness + beat,
            beat,
        }
    }

    pub(crate) fn step(&self, now: Instant) -> HandoverStep {
        let left = self.remaining(now);
        if left.is_zero() {
            HandoverStep::Refuse
        } else {
            HandoverStep::Recheck(left.min(self.beat))
        }
    }

    pub(crate) fn remaining(&self, now: Instant) -> Duration {
        self.deadline.saturating_duration_since(now)
    }
}

pub(crate) async fn claim_with_handover(
    pool: &PgPool,
    config: &EngineConfig,
    readiness: &ReadinessHandle,
) -> Result<(), EngineError> {
    let service_version = config.schema_service_version();
    let mut handover: Option<HandoverDeadline> = None;
    loop {
        let (live_engine, live_service) = match claim_schema_version(
            pool,
            ENGINE_SCHEMA_VERSION,
            service_version,
            config.pod_id.as_str(),
            config.schema_version_liveness,
        )
        .await
        {
            Ok(()) => {
                if handover.is_some() {
                    tracing::info!(
                        engine_version = ENGINE_SCHEMA_VERSION,
                        service_version,
                        "the other schema version's heartbeat lapsed within the handover wait: \
                         this pod claimed the schema version singleton",
                    );
                }
                return Ok(());
            }
            Err(EngineError::SchemaVersionConflict {
                live_engine,
                live_service,
                ..
            }) => (live_engine, live_service),
            Err(other) => return Err(other),
        };
        let now = Instant::now();
        let deadline = *handover.get_or_insert_with(|| {
            HandoverDeadline::from_first_conflict(now, config.schema_version_liveness, config.beat)
        });
        let HandoverStep::Recheck(pause) = deadline.step(now) else {
            return Err(EngineError::SchemaVersionConflict {
                live_engine,
                live_service,
                engine_version: ENGINE_SCHEMA_VERSION.to_string(),
                service_version: service_version.to_string(),
            });
        };
        let reason = handover_wait_reason(
            &live_engine,
            &live_service,
            ENGINE_SCHEMA_VERSION,
            service_version,
            deadline.remaining(now),
        );
        tracing::warn!(
            reason = %reason,
            "a different schema version is live: waiting for its heartbeat to lapse",
        );
        readiness.set_not_ready(reason);
        tokio::time::sleep(pause).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIVENESS: Duration = Duration::from_secs(30);
    const BEAT: Duration = Duration::from_secs(1);

    fn anchored() -> (Instant, HandoverDeadline) {
        let first = Instant::now();
        (
            first,
            HandoverDeadline::from_first_conflict(first, LIVENESS, BEAT),
        )
    }

    #[test]
    fn the_first_conflict_rechecks_one_beat_later() {
        let (first, deadline) = anchored();
        assert_eq!(deadline.step(first), HandoverStep::Recheck(BEAT));
        assert_eq!(deadline.remaining(first), LIVENESS + BEAT);
    }

    #[test]
    fn a_heartbeat_written_just_after_the_first_check_still_ages_out_before_the_deadline() {
        let (first, deadline) = anchored();
        let last_old_beat = first + BEAT - Duration::from_millis(1);
        let lapses_at = last_old_beat + LIVENESS;
        assert!(
            matches!(deadline.step(lapses_at), HandoverStep::Recheck(_)),
            "a pod that beat once more just after the first check must not be refused before \
             that beat has had the whole liveness to lapse"
        );
    }

    #[test]
    fn the_last_recheck_lands_on_the_deadline_rather_than_past_it() {
        let (first, deadline) = anchored();
        let half_a_beat_short = first + LIVENESS + BEAT / 2;
        assert_eq!(
            deadline.step(half_a_beat_short),
            HandoverStep::Recheck(BEAT / 2)
        );
    }

    #[test]
    fn a_conflict_seen_at_or_after_the_deadline_is_refused() {
        let (first, deadline) = anchored();
        assert_eq!(deadline.step(first + LIVENESS + BEAT), HandoverStep::Refuse);
        assert_eq!(
            deadline.step(first + LIVENESS + BEAT * 3),
            HandoverStep::Refuse
        );
    }
}
