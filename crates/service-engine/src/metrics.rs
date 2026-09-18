pub const PREFIX: &str = "service_engine_";

pub const PASS_DURATION_SECONDS: &str = "service_engine_pass_duration_seconds";
pub const PASS_IMPACTS: &str = "service_engine_pass_impacts";
pub const PASS_DELTAS: &str = "service_engine_pass_deltas";
pub const PASS_OVERFLOWS_TOTAL: &str = "service_engine_pass_overflows_total";
pub const IMPACTS_COMMITTED_TOTAL: &str = "service_engine_impacts_committed_total";
pub const IMPACTS_RECEIVED_TOTAL: &str = "service_engine_impacts_received_total";
pub const RESETS_TOTAL: &str = "service_engine_resets_total";
pub const SESSIONS: &str = "service_engine_sessions";
pub const SESSIONS_ENDED_TOTAL: &str = "service_engine_sessions_ended_total";
pub const PENDING_SESSIONS: &str = "service_engine_pending_sessions";
pub const COHORTS: &str = "service_engine_cohorts";
pub const RELAY_DRAINS_TOTAL: &str = "service_engine_relay_drains_total";
pub const RELAY_ROWS_TOTAL: &str = "service_engine_relay_rows_total";
pub const LEADER_SLOT_CLAIMS_TOTAL: &str = "service_engine_leader_slot_claims_total";
pub const CRON_RUNS_TOTAL: &str = "service_engine_cron_runs_total";
pub const CRON_DURATION_SECONDS: &str = "service_engine_cron_duration_seconds";
pub const CHUNK_FLUSH_SIZE: &str = "service_engine_chunk_flush_size";
pub const CHUNK_FLUSH_DURATION_SECONDS: &str = "service_engine_chunk_flush_duration_seconds";
pub const CHUNK_CONFLICTS_TOTAL: &str = "service_engine_chunk_conflicts_total";
pub const NOTIFICATION_QUEUE_USAGE: &str = "service_engine_notification_queue_usage";
pub const TRANSPORT_RECONNECTS_TOTAL: &str = "service_engine_transport_reconnects_total";
pub const MIRROR_RESTARTS_TOTAL: &str = "service_engine_mirror_restarts_total";
pub const BLOBS_REAPED_TOTAL: &str = "service_engine_blobs_reaped_total";
pub const DEAD_LETTERS_TOTAL: &str = "service_engine_dead_letters_total";
pub const OUTBOX_PENDING: &str = "service_engine_outbox_pending";
pub const OUTBOX_OLDEST_AGE_SECONDS: &str = "service_engine_outbox_oldest_age_seconds";
pub const DEPENDENCY_UP: &str = "service_engine_dependency_up";
pub const LEADER: &str = "service_engine_leader";

pub const ALL: &[&str] = &[
    PASS_DURATION_SECONDS,
    PASS_IMPACTS,
    PASS_DELTAS,
    PASS_OVERFLOWS_TOTAL,
    IMPACTS_COMMITTED_TOTAL,
    IMPACTS_RECEIVED_TOTAL,
    RESETS_TOTAL,
    SESSIONS,
    SESSIONS_ENDED_TOTAL,
    PENDING_SESSIONS,
    COHORTS,
    RELAY_DRAINS_TOTAL,
    RELAY_ROWS_TOTAL,
    LEADER_SLOT_CLAIMS_TOTAL,
    CRON_RUNS_TOTAL,
    CRON_DURATION_SECONDS,
    CHUNK_FLUSH_SIZE,
    CHUNK_FLUSH_DURATION_SECONDS,
    CHUNK_CONFLICTS_TOTAL,
    NOTIFICATION_QUEUE_USAGE,
    TRANSPORT_RECONNECTS_TOTAL,
    MIRROR_RESTARTS_TOTAL,
    BLOBS_REAPED_TOTAL,
    DEAD_LETTERS_TOTAL,
    OUTBOX_PENDING,
    OUTBOX_OLDEST_AGE_SECONDS,
    DEPENDENCY_UP,
];

pub const LABEL_SERVICE: &str = "service";
pub const LABEL_POD: &str = "pod";
pub const LABEL_JOB: &str = "job";
pub const LABEL_MIRROR: &str = "mirror";
pub const LABEL_OUTCOME: &str = "outcome";
pub const LABEL_REASON: &str = "reason";
pub const LABEL_DEPENDENCY: &str = "dependency";
pub const LABEL_SOURCE: &str = "source";
pub const LABEL_KIND: &str = "kind";
pub const LABEL_NAME: &str = "name";

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn every_engine_metric_carries_the_engines_prefix() {
        for name in ALL {
            assert!(
                name.starts_with(PREFIX),
                "{name} does not start with {PREFIX}"
            );
        }
    }

    #[test]
    fn no_engine_metric_name_is_declared_twice() {
        let unique: BTreeSet<_> = ALL.iter().collect();
        assert_eq!(unique.len(), ALL.len());
    }

    #[test]
    fn the_leader_gauge_carries_the_engines_prefix() {
        assert!(LEADER.starts_with(PREFIX));
    }
}
