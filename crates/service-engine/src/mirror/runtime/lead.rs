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

/// What the watermark the leader holds says about a standby's own boot read.
enum Caught {
    Yes,
    NotYet,
    /// The held identity is not the one this pod read. A replaced bucket is a
    /// first adoption, never a failure, so this is a wait — never an error.
    Replaced,
}

impl<K, Pr> MirrorRuntime<K, Pr>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    Pr: Project<K>,
{
    pub(super) async fn converge_as_leader_or_wait(
        &self,
        touched: Vec<K>,
    ) -> Result<(), EngineError> {
        let mut touched = touched;
        let mut target = self.read_revision.read().await.clone();
        let Some(gate) = &self.leader else {
            self.project_under_lease(touched, &target, Mark::Adopt)
                .await?;
            return Ok(());
        };
        let mut reread = false;
        loop {
            if self
                .project_under_lease(touched.clone(), &target, Mark::Adopt)
                .await?
            {
                return Ok(());
            }
            match self.caught_up(&target).await? {
                Caught::Yes => return Ok(()),
                // The leader holds another stream identity. Either this pod's
                // read is the stale one — so it re-reads, once — or the leader
                // has yet to adopt the stream this pod just read, which is the
                // leader's own next read to do. Neither is a failure:
                // replacement is adoption, and the standby waits for it exactly
                // as it waits for a sequence.
                Caught::Replaced if !reread => {
                    reread = true;
                    touched = self.absorb_full_read().await?;
                    target = self.read_revision.read().await.clone();
                }
                Caught::NotYet | Caught::Replaced => tokio::time::sleep(gate.beat).await,
            }
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
    async fn caught_up(&self, target: &Revisions) -> Result<Caught, EngineError> {
        let mut conn = self.pool.acquire().await?;
        for (bucket, target) in target {
            let Some(held) = watermark::read(&mut conn, self.name.as_str(), bucket).await? else {
                return Ok(Caught::NotYet);
            };
            // A row written before the identity column carries none, and a
            // leader still on that release will never write one. It is read
            // under those semantics — the sequence alone — and takes an identity
            // the moment this pod holds the lease; holding out for one would
            // stall every rolling upgrade behind the old leader.
            if let Some(created) = held.stream_created_at.as_deref()
                && created != target.created
            {
                return Ok(Caught::Replaced);
            }
            if held.revision() < target.revision {
                return Ok(Caught::NotYet);
            }
        }
        Ok(Caught::Yes)
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
