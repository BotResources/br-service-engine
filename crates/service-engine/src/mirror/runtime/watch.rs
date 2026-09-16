use std::hash::Hash;

use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use tokio::time::MissedTickBehavior;

use crate::error::EngineError;

use super::super::builder::Update;
use super::super::projection::Project;
use super::super::watermark::{self, Mark, Snapshot};
use super::{MirrorRuntime, Revisions, dedup, merge_forward};

/// How many times a watch start may rescan a bucket whose read boundary is
/// zero. A JetStream sequence never returns to zero, so one rescan settles it;
/// the bound is there so a broker answering oddly cannot spin this loop.
const ZERO_BOUNDARY_RESCANS: u32 = 3;

/// One watch's items, followed by the marker that names it when it ends.
/// `select_all` drops an exhausted stream silently, so a bucket could stop being
/// watched with nothing said until the next periodic reconcile; the marker turns
/// that silence into a reopen.
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
            // The watches exist now. A bucket that resumed from zero opened a
            // future-only `watch_all()`, so a put that landed between the
            // boundary and this subscription reached nobody. Fresh metadata read
            // *after* the subscription closes that window: whatever the bucket
            // gained is either already on the open watch or is read here, and a
            // reconcile settles both. Zero is every consumer's first boot, so
            // this window is the common case, not the rare one.
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

    /// `watch_all_from(0)` is future-only in the pinned client, so a bucket
    /// whose read boundary is zero would hide a write that landed between the
    /// scan and the watch. Rescan first, before opening anything, so no watch is
    /// opened only to be dropped, and resume from a boundary that is either
    /// nonzero or a genuinely empty bucket with nothing to miss.
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

    /// Whether a bucket this mirror is resuming from zero has gained a sequence
    /// since. A JetStream sequence never returns to zero, so an answer of `true`
    /// is final: the bucket leaves the zero case for the life of the stream.
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

    /// Runs the open watches until every one of them is gone (`true`), or until
    /// one bucket needs its watch reopened (`false`): the periodic scan replaced
    /// the boundary they resume from, or a single watch ended under them.
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
                    // Reopen from the new boundary: whatever is buffered behind
                    // these watches predates the scan that just replaced it.
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
        // A watch opens only on a bucket a full read has snapshotted. If that
        // ever stops holding, the event is skipped rather than panicked on: the
        // next read is what would give it an identity to advance against.
        let Some(created) = snapshotted else {
            return Ok(());
        };
        let touched = {
            let mut shadows = self.shadows.write().await;
            // Key the change on both sides of it. A retract whose projection key
            // lived only in the payload has no key left once the value is gone,
            // and a put that moves a row to another key must retire the one it
            // left; keying before and after covers both without a store scan.
            let mut touched = (self.keyed_by)(&shadows, &update.change);
            (update.apply)(&mut shadows);
            touched.extend((self.keyed_by)(&shadows, &update.change));
            dedup(touched)
        };
        let mut advance = Revisions::new();
        advance.insert(
            update.bucket.to_string(),
            Snapshot {
                created,
                revision: update.revision,
            },
        );
        merge_forward(&mut *self.last_seen.write().await, &advance);
        self.project_under_lease(touched, &advance, Mark::Advance)
            .await?;
        Ok(())
    }
}
