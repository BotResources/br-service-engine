use std::hash::Hash;

use sqlx::PgConnection;

use crate::error::EngineError;
use crate::housekeeping::leader::try_advisory_xact_lock;
use crate::impact::Impact;

use super::super::leader::hold_lease;
use super::super::projection::{Project, Projection};
use super::super::shadow::Shadows;
use super::super::watermark::{self, Mark};
use super::{MirrorRuntime, Revisions};

impl<K, Pr> MirrorRuntime<K, Pr>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    Pr: Project<K>,
{
    pub(super) async fn converge_as_leader_or_wait(
        &self,
        touched: Vec<K>,
        read_revision: &Revisions,
    ) -> Result<(), EngineError> {
        let Some(gate) = &self.leader else {
            self.project_under_lease(touched, read_revision, Mark::Adopt)
                .await?;
            return Ok(());
        };
        loop {
            if self
                .project_under_lease(touched.clone(), read_revision, Mark::Adopt)
                .await?
            {
                return Ok(());
            }
            if self.watermark_caught_up(read_revision).await? {
                return Ok(());
            }
            tokio::time::sleep(gate.beat).await;
        }
    }

    /// Projects and commits the watermark in one transaction. Nothing here talks
    /// to NATS: a broker round-trip inside this transaction would pin the
    /// advisory lock and the `leader_slot` row for its whole timeout and stall
    /// every other pod's beat.
    pub(super) async fn project_under_lease(
        &self,
        keys: Vec<K>,
        advance: &Revisions,
        mark: Mark,
    ) -> Result<bool, EngineError> {
        if keys.is_empty() && advance.is_empty() {
            return Ok(true);
        }
        let mut tx = self.pool.begin().await?;
        if let Some(gate) = &self.leader {
            if !try_advisory_xact_lock(&mut tx, gate.advisory_key).await? {
                let _ = tx.rollback().await;
                return Ok(false);
            }
            if !hold_lease(&mut tx, &gate.slot_name, gate.pod.as_str(), gate.lease).await? {
                let _ = tx.rollback().await;
                return Ok(false);
            }
        }
        let mut impacts = Vec::new();
        {
            let shadows = self.shadows.read().await;
            self.project_into(&mut tx, &shadows, keys, &mut impacts)
                .await?;
        }
        for (bucket, snapshot) in advance {
            watermark::write(&mut tx, self.name.as_str(), bucket, snapshot, mark).await?;
        }
        let committed = impacts.len();
        if !impacts.is_empty() {
            self.transport.stage_in(&mut tx, &impacts).await?;
        }
        tx.commit().await?;
        crate::observe::record_impacts_committed(committed);
        Ok(true)
    }

    /// A standby is converged once its shadows are loaded and the leader has
    /// committed at least the sequence the standby's own boot read captured, on
    /// the identity that read saw. It compares what it holds against what it has
    /// already read, so waiting costs no `STREAM.INFO` per beat.
    async fn watermark_caught_up(&self, read_revision: &Revisions) -> Result<bool, EngineError> {
        let mut conn = self.pool.acquire().await?;
        for (bucket, target) in read_revision {
            let Some(held) = watermark::read(&mut conn, self.name.as_str(), bucket).await? else {
                return Ok(false);
            };
            match held.stream_created_at.as_deref() {
                // No identity yet: a leader upgraded from a release before the
                // identity column, still to adopt one. Keep waiting.
                None => return Ok(false),
                Some(created) if created != target.created => {
                    // The leader converged on another stream. This standby's
                    // boot read is stale, so it re-reads rather than wait for a
                    // sequence that belongs to a bucket that no longer exists.
                    return Err(EngineError::Config(format!(
                        "mirror {}, bucket {bucket}: the leader adopted another stream identity; \
                         re-reading the bucket",
                        self.name
                    )));
                }
                Some(_) => {}
            }
            if held.revision() < target.revision {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) async fn on_beat(&self, was_leader: &mut bool) -> Result<(), EngineError> {
        let Some(gate) = &self.leader else {
            return Ok(());
        };
        let now_leader = {
            let mut tx = self.pool.begin().await?;
            let held = hold_lease(&mut tx, &gate.slot_name, gate.pod.as_str(), gate.lease).await?;
            if held {
                tx.commit().await?;
            } else {
                let _ = tx.rollback().await;
            }
            held
        };
        if now_leader && !*was_leader {
            self.reproject_shadow().await?;
        }
        *was_leader = now_leader;
        Ok(())
    }

    async fn reproject_shadow(&self) -> Result<(), EngineError> {
        let touched = {
            let shadows = self.shadows.read().await;
            let mut changes = Vec::new();
            for consumption in self.consumptions.iter() {
                changes.extend((consumption.snapshot)(&shadows));
            }
            self.touched_for(&shadows, &changes).await?
        };
        let advance = self.last_seen.read().await.clone();
        self.project_under_lease(touched, &advance, Mark::Advance)
            .await?;
        Ok(())
    }

    async fn project_into(
        &self,
        conn: &mut PgConnection,
        shadows: &Shadows,
        keys: Vec<K>,
        impacts: &mut Vec<Impact>,
    ) -> Result<(), EngineError> {
        for key in keys {
            let cx = Projection::new(conn, shadows, impacts);
            self.project
                .project(cx, key)
                .await
                .map_err(|error| EngineError::Service(Box::new(error)))?;
        }
        Ok(())
    }
}
