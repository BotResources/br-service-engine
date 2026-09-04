mod report;
mod run;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;

use crate::config::{DEFAULT_BEAT, DEFAULT_LEASE, EngineConfig};
use crate::cron::{CronJob, NextFire, Schedule};
use crate::error::CronError;
use crate::housekeeping::leader::{self, Lease, SlotName};
use crate::name::{JobName, PodId};
use crate::time::{self, Timestamp};

pub use report::{CronReport, JobRecord};
pub use run::CronRound;

use run::InFlight;

pub const DEFAULT_SLOT_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

struct Entry {
    name: JobName,
    schedule: Schedule,
    job: Arc<dyn CronJob>,
    running: Option<InFlight>,
    record: JobRecord,
}

pub struct CronRuntime {
    pod: PodId,
    beat: Duration,
    lease: Duration,
    retention: Duration,
    entries: Vec<Entry>,
}

impl CronRuntime {
    pub fn new(pod: PodId) -> Self {
        Self {
            pod,
            beat: DEFAULT_BEAT,
            lease: DEFAULT_LEASE,
            retention: DEFAULT_SLOT_RETENTION,
            entries: Vec::new(),
        }
    }

    pub fn from_config(config: &EngineConfig) -> Self {
        Self::new(config.pod_id.clone())
            .with_beat(config.beat)
            .with_lease(config.lease)
    }

    pub fn with_beat(mut self, beat: Duration) -> Self {
        self.beat = beat;
        self
    }

    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    pub fn with_slot_retention(mut self, retention: Duration) -> Self {
        self.set_slot_retention(retention);
        self
    }

    pub fn set_slot_retention(&mut self, retention: Duration) {
        self.retention = retention;
    }

    pub const fn slot_retention(&self) -> Duration {
        self.retention
    }

    pub fn register<J: CronJob>(&mut self, job: J) -> Result<(), CronError> {
        self.register_erased(Arc::new(job))
    }

    pub fn register_erased(&mut self, job: Arc<dyn CronJob>) -> Result<(), CronError> {
        let name = job.name();
        if self.entries.iter().any(|entry| entry.name == name) {
            return Err(CronError::DuplicateJob { name });
        }
        let schedule = job.schedule();
        let after = time::now();
        schedule.previous_fire(after, self.beat)?;
        if matches!(schedule.next_fire(after, self.beat)?, NextFire::Never) {
            return Err(CronError::NoNextFire { job: name, after });
        }
        self.entries.push(Entry {
            name,
            schedule,
            job,
            running: None,
            record: JobRecord::default(),
        });
        Ok(())
    }

    pub fn names(&self) -> Vec<JobName> {
        self.entries
            .iter()
            .map(|entry| entry.name.clone())
            .collect()
    }

    pub fn report(&self) -> CronReport {
        CronReport::from_iter(
            self.entries
                .iter()
                .map(|entry| (entry.name.clone(), entry.record.clone())),
        )
    }

    pub fn running(&self) -> usize {
        self.entries.iter().filter(|e| e.running.is_some()).count()
    }

    pub fn due_slot(&self, name: &JobName, now: Timestamp) -> Option<Timestamp> {
        self.entries
            .iter()
            .find(|entry| &entry.name == name)
            .and_then(|entry| match entry.schedule.previous_fire(now, self.beat) {
                Ok(NextFire::At(slot)) => Some(slot),
                _ => None,
            })
    }

    pub async fn beat(&mut self, pg: &PgPool) -> CronRound {
        let now = time::now();
        let mut round = CronRound::default();
        for index in 0..self.entries.len() {
            self.settle(pg, index, &mut round).await;
            if self.entries[index].running.is_some() {
                round.running += 1;
                continue;
            }
            self.start(pg, index, now, &mut round).await;
        }
        round
    }

    async fn settle(&mut self, pg: &PgPool, index: usize, round: &mut CronRound) {
        let Some(flight) = self.entries[index].running.take() else {
            return;
        };
        let name = self.entries[index].name.clone();
        match flight.settle(pg, &name, self.lease).await {
            run::Settled::Running(flight) => self.entries[index].running = Some(flight),
            run::Settled::Done(outcome) => {
                round.completed += 1;
                if outcome.failed {
                    round.failed += 1;
                }
                crate::observe::record_cron_run(&name, outcome.duration, outcome.failed);
                self.entries[index].record.observe(&outcome);
            }
        }
    }

    async fn start(&mut self, pg: &PgPool, index: usize, now: Timestamp, round: &mut CronRound) {
        let slot = match self.entries[index].schedule.previous_fire(now, self.beat) {
            Ok(NextFire::At(slot)) => slot,
            Ok(NextFire::Never) => {
                round.idle += 1;
                return;
            }
            Err(error) => {
                self.entries[index].record.refuse(&error);
                round.refused += 1;
                return;
            }
        };
        if now
            .signed_duration_since(slot)
            .to_std()
            .is_ok_and(|age| age >= self.retention)
        {
            round.expired += 1;
            return;
        }
        let name = self.entries[index].name.clone();
        match self.claim(pg, &name, slot).await {
            Ok(None) => round.skipped += 1,
            Ok(Some(lease)) => {
                let job = self.entries[index].job.clone();
                self.entries[index].running = Some(InFlight::spawn(job, pg.clone(), lease));
                round.ran += 1;
                round.running += 1;
            }
            Err(error) => {
                self.entries[index].record.refuse(&error);
                round.refused += 1;
            }
        }
    }

    async fn claim(
        &self,
        pg: &PgPool,
        name: &JobName,
        slot: Timestamp,
    ) -> Result<Option<Lease>, CronError> {
        let mut conn = pg.acquire().await?;
        leader::claim_slot_at(
            &mut conn,
            SlotName::Cron(name.clone()),
            slot,
            &self.pod,
            self.lease,
        )
        .await
        .map_err(|error| CronError::Job(Box::new(error)))
    }
}

impl std::fmt::Debug for CronRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CronRuntime")
            .field("pod", &self.pod)
            .field("beat", &self.beat)
            .field("lease", &self.lease)
            .field("jobs", &self.names())
            .finish_non_exhaustive()
    }
}

impl FromIterator<(JobName, JobRecord)> for CronReport {
    fn from_iter<I: IntoIterator<Item = (JobName, JobRecord)>>(iter: I) -> Self {
        CronReport::new(BTreeMap::from_iter(iter))
    }
}

#[cfg(test)]
mod tests;
