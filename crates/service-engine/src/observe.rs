use std::time::Duration;

use crate::housekeeping::beat::BeatRound;
use crate::metrics::{
    CHUNK_CONFLICTS_TOTAL, CHUNK_FLUSH_DURATION_SECONDS, CHUNK_FLUSH_SIZE, COHORTS,
    CRON_DURATION_SECONDS, CRON_RUNS_TOTAL, LABEL_JOB, LABEL_MIRROR, LABEL_OUTCOME,
    LEADER_SLOT_CLAIMS_TOTAL, MIRROR_RESTARTS_TOTAL, NOTIFICATION_QUEUE_USAGE, PASS_DELTAS,
    PASS_DURATION_SECONDS, PASS_IMPACTS, PASS_OVERFLOWS_TOTAL, PENDING_SESSIONS,
    RELAY_DRAINS_TOTAL, RELAY_ROWS_TOTAL, RESETS_TOTAL, SESSIONS, TRANSPORT_RECONNECTS_TOTAL,
};
use crate::name::{JobName, MirrorName};
use crate::render::pass::PassReport;

pub fn record_pass(report: &PassReport, duration_seconds: f64, sessions: usize, pending: usize) {
    metrics::histogram!(PASS_DURATION_SECONDS).record(duration_seconds);
    metrics::histogram!(PASS_IMPACTS).record(report.impacts as f64);
    metrics::histogram!(PASS_DELTAS).record(report.deltas as f64);
    if report.resets > 0 {
        metrics::counter!(RESETS_TOTAL).increment(report.resets as u64);
    }
    metrics::gauge!(SESSIONS).set(sessions as f64);
    metrics::gauge!(PENDING_SESSIONS).set(pending as f64);
    metrics::gauge!(COHORTS).set(report.cohorts as f64);
}

pub fn record_resets(resets: usize) {
    if resets > 0 {
        metrics::counter!(RESETS_TOTAL).increment(resets as u64);
    }
}

pub fn record_overflow() {
    metrics::counter!(PASS_OVERFLOWS_TOTAL).increment(1);
}

pub fn record_reconnect() {
    metrics::counter!(TRANSPORT_RECONNECTS_TOTAL).increment(1);
}

pub fn record_cron_run(job: &JobName, duration: Duration, failed: bool) {
    let outcome = if failed { "failed" } else { "ok" };
    metrics::histogram!(CRON_DURATION_SECONDS, LABEL_JOB => job.as_str().to_string(), LABEL_OUTCOME => outcome)
        .record(duration.as_secs_f64());
}

pub fn record_chunk_flush(size: usize, duration: Duration) {
    metrics::histogram!(CHUNK_FLUSH_SIZE).record(size as f64);
    metrics::histogram!(CHUNK_FLUSH_DURATION_SECONDS).record(duration.as_secs_f64());
}

pub fn record_chunk_conflicts(count: usize) {
    if count > 0 {
        metrics::counter!(CHUNK_CONFLICTS_TOTAL).increment(count as u64);
    }
}

pub fn record_mirror_restart(mirror: &MirrorName) {
    metrics::counter!(MIRROR_RESTARTS_TOTAL, LABEL_MIRROR => mirror.as_str().to_string())
        .increment(1);
}

pub fn record_beat(round: &BeatRound) {
    if round.relays.ran > 0 {
        metrics::counter!(RELAY_DRAINS_TOTAL).increment(round.relays.ran as u64);
    }
    if round.relays.rows > 0 {
        metrics::counter!(RELAY_ROWS_TOTAL).increment(round.relays.rows as u64);
    }
    if round.cron.ran > 0 {
        metrics::counter!(CRON_RUNS_TOTAL).increment(round.cron.ran as u64);
    }
    let won = round.cron.ran + round.relays.slot_won;
    if won > 0 {
        metrics::counter!(LEADER_SLOT_CLAIMS_TOTAL, LABEL_OUTCOME => "won").increment(won as u64);
    }
    let skipped = round.cron.skipped + round.relays.slot_skipped;
    if skipped > 0 {
        metrics::counter!(LEADER_SLOT_CLAIMS_TOTAL, LABEL_OUTCOME => "skipped")
            .increment(skipped as u64);
    }
    if let Some(usage) = round.queue_usage {
        metrics::gauge!(NOTIFICATION_QUEUE_USAGE).set(usage);
    }
}
