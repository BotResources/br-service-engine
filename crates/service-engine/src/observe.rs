use std::sync::OnceLock;
use std::time::Duration;

use metrics::Label;

use crate::config::EngineConfig;
use crate::housekeeping::beat::BeatRound;
use crate::metrics::{
    CHUNK_CONFLICTS_TOTAL, CHUNK_FLUSH_DURATION_SECONDS, CHUNK_FLUSH_SIZE, COHORTS,
    CRON_DURATION_SECONDS, CRON_RUNS_TOTAL, DEPENDENCY_UP, IMPACTS_COMMITTED_TOTAL,
    IMPACTS_RECEIVED_TOTAL, LABEL_DEPENDENCY, LABEL_JOB, LABEL_MIRROR, LABEL_OUTCOME, LABEL_POD,
    LABEL_REASON, LABEL_SERVICE, LEADER_SLOT_CLAIMS_TOTAL, MIRROR_RESTARTS_TOTAL,
    NOTIFICATION_QUEUE_USAGE, PASS_DELTAS, PASS_DURATION_SECONDS, PASS_IMPACTS,
    PASS_OVERFLOWS_TOTAL, PENDING_SESSIONS, RELAY_DRAINS_TOTAL, RELAY_ROWS_TOTAL, RESETS_TOTAL,
    SESSIONS, SESSIONS_ENDED_TOTAL, TRANSPORT_RECONNECTS_TOTAL,
};
use crate::name::{JobName, MirrorName};
use crate::render::pass::PassReport;

pub const REASON_MAX_AGE: &str = "max_age";
pub const REASON_RECONNECT: &str = "reconnect";
pub const REASON_REPAIR: &str = "repair";
pub const REASON_PASS: &str = "pass";

pub const DEP_POSTGRES: &str = "postgres";
pub const DEP_LISTENER: &str = "listener";
pub const DEP_NATS: &str = "nats";
pub const DEP_MIRRORS: &str = "mirrors";

static IDENTITY: OnceLock<Vec<Label>> = OnceLock::new();

pub fn install_identity(config: &EngineConfig) {
    let mut labels = Vec::with_capacity(2);
    if let Some(service) = &config.service {
        labels.push(Label::new(LABEL_SERVICE, service.clone()));
    }
    labels.push(Label::new(LABEL_POD, config.pod_id.as_str().to_string()));
    let _ = IDENTITY.set(labels);
}

fn identity() -> Vec<Label> {
    IDENTITY.get().cloned().unwrap_or_default()
}

fn labelled<const N: usize>(extra: [(&'static str, String); N]) -> Vec<Label> {
    let mut labels = identity();
    labels.reserve(N);
    for (key, value) in extra {
        labels.push(Label::new(key, value));
    }
    labels
}

pub fn record_pass(report: &PassReport, duration_seconds: f64, sessions: usize, pending: usize) {
    metrics::histogram!(PASS_DURATION_SECONDS, identity()).record(duration_seconds);
    metrics::histogram!(PASS_IMPACTS, identity()).record(report.impacts as f64);
    metrics::histogram!(PASS_DELTAS, identity()).record(report.deltas as f64);
    if report.impacts > 0 {
        metrics::counter!(IMPACTS_RECEIVED_TOTAL, identity()).increment(report.impacts as u64);
    }
    if report.resets > 0 {
        metrics::counter!(RESETS_TOTAL, labelled([(LABEL_REASON, REASON_PASS.to_string())]))
            .increment(report.resets as u64);
    }
    metrics::gauge!(SESSIONS, identity()).set(sessions as f64);
    metrics::gauge!(PENDING_SESSIONS, identity()).set(pending as f64);
    metrics::gauge!(COHORTS, identity()).set(report.cohorts as f64);
}

pub fn record_resets(resets: usize, reason: &'static str) {
    if resets > 0 {
        metrics::counter!(RESETS_TOTAL, labelled([(LABEL_REASON, reason.to_string())]))
            .increment(resets as u64);
    }
}

pub fn record_sessions_ended(count: usize, reason: &'static str) {
    if count > 0 {
        metrics::counter!(
            SESSIONS_ENDED_TOTAL,
            labelled([(LABEL_REASON, reason.to_string())])
        )
        .increment(count as u64);
    }
}

pub fn record_impacts_committed(count: usize) {
    if count > 0 {
        metrics::counter!(IMPACTS_COMMITTED_TOTAL, identity()).increment(count as u64);
    }
}

pub fn record_overflow() {
    metrics::counter!(PASS_OVERFLOWS_TOTAL, identity()).increment(1);
}

pub fn record_reconnect() {
    metrics::counter!(TRANSPORT_RECONNECTS_TOTAL, identity()).increment(1);
}

pub fn record_dependency(dependency: &'static str, up: bool) {
    metrics::gauge!(
        DEPENDENCY_UP,
        labelled([(LABEL_DEPENDENCY, dependency.to_string())])
    )
    .set(if up { 1.0 } else { 0.0 });
}

pub fn record_cron_run(job: &JobName, duration: Duration, failed: bool) {
    let outcome = if failed { "failed" } else { "ok" };
    metrics::histogram!(
        CRON_DURATION_SECONDS,
        labelled([
            (LABEL_JOB, job.as_str().to_string()),
            (LABEL_OUTCOME, outcome.to_string()),
        ])
    )
    .record(duration.as_secs_f64());
}

pub fn record_chunk_flush(size: usize, duration: Duration) {
    metrics::histogram!(CHUNK_FLUSH_SIZE, identity()).record(size as f64);
    metrics::histogram!(CHUNK_FLUSH_DURATION_SECONDS, identity()).record(duration.as_secs_f64());
}

pub fn record_chunk_conflicts(count: usize) {
    if count > 0 {
        metrics::counter!(CHUNK_CONFLICTS_TOTAL, identity()).increment(count as u64);
    }
}

pub fn record_mirror_restart(mirror: &MirrorName) {
    metrics::counter!(
        MIRROR_RESTARTS_TOTAL,
        labelled([(LABEL_MIRROR, mirror.as_str().to_string())])
    )
    .increment(1);
}

pub fn record_beat(round: &BeatRound) {
    if round.relays.ran > 0 {
        metrics::counter!(RELAY_DRAINS_TOTAL, identity()).increment(round.relays.ran as u64);
    }
    if round.relays.rows > 0 {
        metrics::counter!(RELAY_ROWS_TOTAL, identity()).increment(round.relays.rows as u64);
    }
    if round.cron.ran > 0 {
        metrics::counter!(CRON_RUNS_TOTAL, identity()).increment(round.cron.ran as u64);
    }
    let won = round.cron.ran + round.relays.slot_won;
    if won > 0 {
        metrics::counter!(
            LEADER_SLOT_CLAIMS_TOTAL,
            labelled([(LABEL_OUTCOME, "won".to_string())])
        )
        .increment(won as u64);
    }
    let skipped = round.cron.skipped + round.relays.slot_skipped;
    if skipped > 0 {
        metrics::counter!(
            LEADER_SLOT_CLAIMS_TOTAL,
            labelled([(LABEL_OUTCOME, "skipped".to_string())])
        )
        .increment(skipped as u64);
    }
    if let Some(usage) = round.queue_usage {
        metrics::gauge!(NOTIFICATION_QUEUE_USAGE, identity()).set(usage);
    }
}
