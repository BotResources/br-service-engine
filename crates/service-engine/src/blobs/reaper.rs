use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::{PgPool, Row};
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::blobs::BlobPolicy;
use crate::blobs::store::BlobStore;
use crate::chain::describe;
use crate::error::EngineError;
use crate::schema::TABLE_BLOB;

pub const DEFAULT_REAPER_INTERVAL: Duration = Duration::from_secs(60);
const BATCH: i64 = 256;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReaperRound {
    pub ran: bool,
    pub promoted: u64,
    pub reaped_incomplete: u64,
    pub reaped_orphan: u64,
    pub failures: usize,
}

pub(crate) struct BlobReaper {
    store: Arc<OnceCell<BlobStore>>,
    policies: Vec<(&'static str, BlobPolicy)>,
    interval: Duration,
    due_at: Option<Instant>,
}

impl BlobReaper {
    pub(crate) fn new(
        store: Arc<OnceCell<BlobStore>>,
        policies: Vec<(&'static str, BlobPolicy)>,
    ) -> Self {
        Self {
            store,
            policies,
            interval: DEFAULT_REAPER_INTERVAL,
            due_at: None,
        }
    }

    pub(crate) fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    fn is_due(&self, now: Instant) -> bool {
        self.due_at.is_none_or(|at| now >= at)
    }

    pub(crate) async fn sweep(&mut self, pg: &PgPool) -> ReaperRound {
        let mut round = ReaperRound::default();
        if !self.is_due(Instant::now()) {
            return round;
        }
        self.due_at = Some(Instant::now() + self.interval);
        round.ran = true;
        let Some(store) = self.store.get() else {
            return round;
        };
        for (kind, policy) in &self.policies {
            if let Err(error) = self.reap_kind(pg, store, kind, policy, &mut round).await {
                round.failures += 1;
                tracing::warn!(kind, reason = %describe(&error), "the blob reaper failed a kind");
            }
        }
        round
    }

    async fn reap_kind(
        &self,
        pg: &PgPool,
        store: &BlobStore,
        kind: &str,
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
        self.promote_and_reap_incomplete(pg, store, kind, cutoff, round)
            .await?;
        self.reap_orphans(pg, store, kind, cutoff, round).await
    }

    async fn promote_and_reap_incomplete(
        &self,
        pg: &PgPool,
        store: &BlobStore,
        kind: &str,
        cutoff: chrono::DateTime<chrono::Utc>,
        round: &mut ReaperRound,
    ) -> Result<(), EngineError> {
        let rows = sqlx::query(&format!(
            "SELECT id, object_key, created_at FROM {TABLE_BLOB} \
             WHERE kind = $1 AND state = 'pending' LIMIT $2"
        ))
        .bind(kind)
        .bind(BATCH)
        .fetch_all(pg)
        .await?;
        for row in &rows {
            let id: Uuid = row.get("id");
            let object_key: String = row.get("object_key");
            let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
            match store.object().head_size(&object_key).await? {
                Some(size) => {
                    let done = sqlx::query(&format!(
                        "UPDATE {TABLE_BLOB} \
                         SET state = 'uploaded', uploaded_at = now(), size = $2 \
                         WHERE id = $1 AND state = 'pending'"
                    ))
                    .bind(id)
                    .bind(size as i64)
                    .execute(pg)
                    .await?;
                    round.promoted += done.rows_affected();
                }
                None if created_at < cutoff => {
                    let done = sqlx::query(&format!(
                        "DELETE FROM {TABLE_BLOB} WHERE id = $1 AND state = 'pending'"
                    ))
                    .bind(id)
                    .execute(pg)
                    .await?;
                    round.reaped_incomplete += done.rows_affected();
                }
                None => {}
            }
        }
        Ok(())
    }

    async fn reap_orphans(
        &self,
        pg: &PgPool,
        store: &BlobStore,
        kind: &str,
        cutoff: chrono::DateTime<chrono::Utc>,
        round: &mut ReaperRound,
    ) -> Result<(), EngineError> {
        let rows = sqlx::query(&format!(
            "SELECT id, object_key FROM {TABLE_BLOB} \
             WHERE kind = $1 AND state = 'orphaned' AND detached_at < $2 LIMIT $3"
        ))
        .bind(kind)
        .bind(cutoff)
        .bind(BATCH)
        .fetch_all(pg)
        .await?;
        for row in &rows {
            let id: Uuid = row.get("id");
            let object_key: String = row.get("object_key");
            store.object().delete_object(&object_key).await?;
            let done = sqlx::query(&format!(
                "DELETE FROM {TABLE_BLOB} WHERE id = $1 AND state = 'orphaned'"
            ))
            .bind(id)
            .execute(pg)
            .await?;
            round.reaped_orphan += done.rows_affected();
        }
        Ok(())
    }
}
