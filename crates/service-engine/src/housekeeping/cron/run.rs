use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::task::JoinHandle;

use crate::chain::describe;
use crate::cron::CronJob;
use crate::housekeeping::leader::{self, Lease};
use crate::name::JobName;
use crate::time::Timestamp;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CronRound {
    pub ran: usize,
    pub running: usize,
    pub completed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub idle: usize,
    pub expired: usize,
    pub refused: usize,
}

#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub slot: Timestamp,
    pub duration: Duration,
    pub failed: bool,
    pub reason: Option<String>,
}

pub(super) struct InFlight {
    handle: JoinHandle<Option<String>>,
    lease: Lease,
    started: Instant,
}

pub(super) enum Settled {
    Running(InFlight),
    Done(RunOutcome),
}

impl InFlight {
    pub(super) fn spawn(job: Arc<dyn CronJob>, pg: PgPool, lease: Lease) -> Self {
        let handle = tokio::spawn(async move {
            match job.run(&pg).await {
                Ok(()) => None,
                Err(error) => Some(describe(&error)),
            }
        });
        Self {
            handle,
            lease,
            started: Instant::now(),
        }
    }

    pub(super) async fn settle(mut self, pg: &PgPool, name: &JobName, lease: Duration) -> Settled {
        if !self.handle.is_finished() {
            self.renew(pg, name, lease).await;
            return Settled::Running(self);
        }
        let slot = self.lease.slot();
        let duration = self.started.elapsed();
        self.complete(pg, name).await;
        let reason = match self.handle.await {
            Ok(reason) => reason,
            Err(join) => Some(format!("the job task ended abnormally: {join}")),
        };
        Settled::Done(RunOutcome {
            slot,
            duration,
            failed: reason.is_some(),
            reason,
        })
    }

    async fn renew(&mut self, pg: &PgPool, name: &JobName, lease: Duration) {
        match pg.acquire().await {
            Ok(mut conn) => match leader::renew_slot(&mut conn, &mut self.lease, lease).await {
                Ok(true) => {}
                Ok(false) => tracing::warn!(
                    job = %name,
                    slot = %self.lease.slot(),
                    "the lease of a running job was taken over; another pod may run its slot",
                ),
                Err(error) => tracing::warn!(
                    job = %name,
                    reason = %describe(&error),
                    "the lease of a running job could not be renewed",
                ),
            },
            Err(error) => tracing::warn!(
                job = %name,
                reason = %describe(&error),
                "no connection was free to renew the lease of a running job",
            ),
        }
    }

    async fn complete(&self, pg: &PgPool, name: &JobName) {
        match pg.acquire().await {
            Ok(mut conn) => match leader::complete_slot(&mut conn, &self.lease).await {
                Ok(true) => {}
                Ok(false) => tracing::warn!(
                    job = %name,
                    slot = %self.lease.slot(),
                    "the slot of a finished job was already taken over, so it may run twice",
                ),
                Err(error) => tracing::warn!(
                    job = %name,
                    reason = %describe(&error),
                    "the slot of a finished job could not be marked complete",
                ),
            },
            Err(error) => tracing::warn!(
                job = %name,
                reason = %describe(&error),
                "no connection was free to mark a finished job's slot complete",
            ),
        }
    }
}
