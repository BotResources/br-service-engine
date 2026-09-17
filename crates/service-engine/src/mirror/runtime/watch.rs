use std::hash::Hash;

use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use tokio::time::MissedTickBehavior;

use crate::error::EngineError;

use super::super::consumption::{Effect, Update};
use super::super::projection::Project;
use super::super::watermark::{self, Mark, Snapshot};
use super::{MirrorRuntime, Revisions, dedup, merge_forward, wire_version_reason};

const ZERO_BOUNDARY_RESCANS: u32 = 3;

enum Served {
    Update(Result<Update, EngineError>),
    Ended(&'static str),
}

type Watches = Vec<BoxStream<'static, Served>>;

impl<K, Pr> MirrorRuntime<K, Pr>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    Pr: Project<K>,
{
    pub(super) async fn watch_forever(&self) -> Result<(), EngineError> {
        if self.read_revision.read().await.is_empty() {
            self.boot_reconcile().await?;
        }
        loop {
            let resume = self.settled_boundary().await?;
            let watches = self.open_watches(&resume).await?;
            if self.zero_boundary_gained(&resume).await? {
                drop(watches);
                self.periodic_reconcile().await?;
                continue;
            }
            if self.serve(watches).await? {
                return Ok(());
            }
        }
    }

    async fn open_watches(&self, resume: &Revisions) -> Result<Watches, EngineError> {
        let mut watches = Watches::new();
        for consumption in self.consumptions.iter() {
            let from = match resume.get(consumption.bucket) {
                Some(snapshot) if snapshot.revision > 0 => snapshot.revision.saturating_add(1),
                _ => 0,
            };
            let bucket = consumption.bucket;
            let opened = (consumption.open_watch)(self.nats.clone(), from).await?;
            watches.push(
                opened
                    .map(Served::Update)
                    .chain(futures_util::stream::once(
                        async move { Served::Ended(bucket) },
                    ))
                    .boxed(),
            );
        }
        Ok(watches)
    }

    async fn settled_boundary(&self) -> Result<Revisions, EngineError> {
        for _ in 0..ZERO_BOUNDARY_RESCANS {
            let resume = self.read_revision.read().await.clone();
            if !self.zero_boundary_gained(&resume).await? {
                return Ok(resume);
            }
            self.periodic_reconcile().await?;
        }
        Ok(self.read_revision.read().await.clone())
    }

    async fn zero_boundary_gained(&self, resume: &Revisions) -> Result<bool, EngineError> {
        for (bucket, snapshot) in resume {
            if snapshot.revision == 0
                && watermark::snapshot(&self.nats, self.name.as_str(), bucket)
                    .await?
                    .revision
                    > 0
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn serve(&self, watches: Watches) -> Result<bool, EngineError> {
        let mut merged = futures_util::stream::select_all(watches);
        let mut beat = tokio::time::interval(self.renew_period());
        beat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        beat.tick().await;
        let mut reconcile = tokio::time::interval(self.reconcile_deadline);
        reconcile.set_missed_tick_behavior(MissedTickBehavior::Skip);
        reconcile.tick().await;
        let mut was_leader = false;
        loop {
            tokio::select! {
                _ = beat.tick() => {
                    self.heartbeat().await?;
                    self.on_beat(&mut was_leader).await?;
                }
                _ = reconcile.tick() => {
                    self.periodic_reconcile().await?;
                    return Ok(false);
                }
                item = merged.next() => match item {
                    None => return Ok(true),
                    Some(Served::Ended(bucket)) => {
                        tracing::warn!(
                            mirror = %self.name,
                            bucket,
                            "a mirror watch ended; reopening the watches from the boundary the \
                             mirror holds",
                        );
                        return Ok(false);
                    }
                    Some(Served::Update(item)) => self.on_watch_event(item?).await?,
                }
            }
        }
    }

    async fn on_watch_event(&self, update: Update) -> Result<(), EngineError> {
        let snapshotted = self
            .read_revision
            .read()
            .await
            .get(update.bucket)
            .map(|snapshot| snapshot.created.clone());
        let Some(created) = snapshotted else {
            return Ok(());
        };
        let mut advance = Revisions::new();
        advance.insert(
            update.bucket.to_string(),
            Snapshot {
                created,
                revision: update.revision,
            },
        );
        match update.effect {
            Effect::Boundary => {
                merge_forward(&mut *self.last_seen.write().await, &advance);
                self.project_under_lease(Vec::new(), &advance, Mark::Advance)
                    .await?;
            }
            Effect::WireVersionRejected { expected, found } => {
                if let Some(change) = &update.change {
                    self.dead_letter(change.key.as_str(), &wire_version_reason(expected, found))
                        .await;
                }
                merge_forward(&mut *self.last_seen.write().await, &advance);
                self.project_under_lease(Vec::new(), &advance, Mark::Advance)
                    .await?;
            }
            Effect::Apply(apply) => {
                let Some(change) = update.change else {
                    return Ok(());
                };
                let touched = {
                    let mut shadows = self.shadows.write().await;
                    let mut touched = (self.keyed_by)(&shadows, &change);
                    apply(&mut shadows);
                    touched.extend((self.keyed_by)(&shadows, &change));
                    dedup(touched)
                };
                merge_forward(&mut *self.last_seen.write().await, &advance);
                self.project_under_lease(touched, &advance, Mark::Advance)
                    .await?;
            }
        }
        self.publish_required_keys().await;
        Ok(())
    }
}
