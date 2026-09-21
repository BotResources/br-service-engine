mod promote;

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::OnceCell;

use crate::blobs::BlobPolicy;
use crate::blobs::store::BlobStore;
use crate::chain::describe;
use crate::error::EngineError;
use crate::housekeeping::leader::{self, SlotName};
use crate::name::PodId;
use crate::pipeline::PolicyRunner;

pub const DEFAULT_REAPER_INTERVAL: Duration = Duration::from_secs(60);
pub(super) const BATCH: i64 = 256;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReaperRound {
    pub ran: bool,
    pub skipped: bool,
    pub promoted: u64,
    pub failed: u64,
    pub reaped_incomplete: u64,
    pub reaped_orphan: u64,
    pub failures: usize,
}

pub(crate) struct BlobReaper {
    store: Arc<OnceCell<BlobStore>>,
    policies: Vec<(&'static str, BlobPolicy)>,
    interval: Duration,
    pod: PodId,
    lease: Duration,
    runner: Option<PolicyRunner>,
}

impl BlobReaper {
    pub(crate) fn new(
        store: Arc<OnceCell<BlobStore>>,
        policies: Vec<(&'static str, BlobPolicy)>,
        pod: PodId,
        lease: Duration,
    ) -> Self {
        Self {
            store,
            policies,
            interval: DEFAULT_REAPER_INTERVAL,
            pod,
            lease,
            runner: None,
        }
    }

    pub(crate) fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    pub(crate) fn with_runner(mut self, runner: PolicyRunner) -> Self {
        self.runner = Some(runner);
        self
    }

    pub(crate) async fn sweep(&mut self, pg: &PgPool) -> ReaperRound {
        let mut round = ReaperRound::default();
        let Some(store) = self.store.get() else {
            return round;
        };
        let mut conn = match pg.acquire().await {
            Ok(conn) => conn,
            Err(error) => {
                round.failures += 1;
                tracing::warn!(reason = %error, "the blob reaper could not acquire a connection");
                return round;
            }
        };
        let lease = match leader::claim_current_slot(
            &mut conn,
            SlotName::Reaper,
            self.interval,
            &self.pod,
            self.lease,
        )
        .await
        {
            Ok(Some(lease)) => lease,
            Ok(None) => {
                round.skipped = true;
                return round;
            }
            Err(error) => {
                round.failures += 1;
                tracing::warn!(reason = %describe(&error), "the blob reaper could not claim its slot");
                return round;
            }
        };
        drop(conn);
        round.ran = true;
        for (kind, policy) in &self.policies {
            if let Err(error) = self.reap_kind(pg, store, kind, policy, &mut round).await {
                round.failures += 1;
                tracing::warn!(kind, reason = %describe(&error), "the blob reaper failed a kind");
            }
        }
        if let Ok(mut conn) = pg.acquire().await {
            let _ = leader::complete_slot(&mut conn, &lease).await;
        }
        round
    }

    async fn reap_kind(
        &self,
        pg: &PgPool,
        store: &BlobStore,
        kind: &'static str,
        policy: &BlobPolicy,
        round: &mut ReaperRound,
    ) -> Result<(), EngineError> {
        let orphan_after = chrono::Duration::from_std(policy.orphan_after).map_err(|_| {
            EngineError::Config(format!(
                "the blob policy orphan_after {:?} does not fit a timestamp cutoff",
                policy.orphan_after
            ))
        })?;
        let cutoff = chrono::Utc::now() - orphan_after;
        self.promote_pending(pg, store, kind, cutoff, round).await?;
        self.reap_detached(pg, store, kind, cutoff, round).await
    }
}
