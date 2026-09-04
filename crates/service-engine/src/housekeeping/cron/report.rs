use std::collections::BTreeMap;
use std::time::Duration;

use crate::chain::describe;
use crate::error::CronError;
use crate::housekeeping::cron::run::RunOutcome;
use crate::name::JobName;
use crate::time::Timestamp;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JobRecord {
    pub runs: u64,
    pub failures: u64,
    pub refusals: u64,
    pub last_slot: Option<Timestamp>,
    pub last_duration: Option<Duration>,
    pub last_reason: Option<String>,
}

impl JobRecord {
    pub(super) fn observe(&mut self, outcome: &RunOutcome) {
        self.runs += 1;
        if outcome.failed {
            self.failures += 1;
        }
        self.last_slot = Some(outcome.slot);
        self.last_duration = Some(outcome.duration);
        self.last_reason = outcome.reason.clone();
    }

    pub(super) fn refuse(&mut self, error: &CronError) {
        self.refusals += 1;
        self.last_reason = Some(describe(error));
    }

    pub fn is_clean(&self) -> bool {
        self.failures == 0 && self.refusals == 0
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CronReport(BTreeMap<JobName, JobRecord>);

impl CronReport {
    pub(super) fn new(records: BTreeMap<JobName, JobRecord>) -> Self {
        Self(records)
    }

    pub fn record(&self, job: &JobName) -> Option<&JobRecord> {
        self.0.get(job)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&JobName, &JobRecord)> {
        self.0.iter()
    }

    pub fn is_clean(&self) -> bool {
        self.0.values().all(JobRecord::is_clean)
    }

    pub fn runs(&self) -> u64 {
        self.0.values().map(|record| record.runs).sum()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
