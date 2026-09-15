mod lead;

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use sqlx::PgPool;
use tokio::sync::RwLock;
use tokio::time::MissedTickBehavior;

use crate::config::DEFAULT_BEAT;
use crate::error::EngineError;
use crate::name::MirrorName;
use crate::nats::{Nats, NatsError};
use crate::transport::ImpactTransport;

const LIVENESS_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

use super::builder::{Consumption, ReconcileKeysFn, Update};
use super::change::Change;
use super::empty::empty_prefix;
use super::handle::MirrorRun;
use super::leader::MirrorGate;
use super::projection::Project;
use super::shadow::Shadows;
use super::watermark::{self, Snapshot};

type KeyedByFn<K> = Arc<dyn Fn(&Shadows, &Change) -> Vec<K> + Send + Sync>;
type Revisions = std::collections::BTreeMap<String, Snapshot>;

pub(super) struct MirrorRuntime<K, Pr: Project<K>> {
    name: MirrorName,
    nats: Nats,
    pool: PgPool,
    transport: Arc<dyn ImpactTransport>,
    consumptions: Arc<Vec<Consumption>>,
    keyed_by: KeyedByFn<K>,
    project: Arc<Pr>,
    reconcile_keys: Option<ReconcileKeysFn<K>>,
    leader: Option<MirrorGate>,
    reconcile_deadline: Duration,
    shadows: Arc<RwLock<Shadows>>,
    read_revision: Arc<RwLock<Revisions>>,
    last_seen: Arc<RwLock<Revisions>>,
}

impl<K, Pr> MirrorRuntime<K, Pr>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    Pr: Project<K>,
{
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        name: MirrorName,
        nats: Nats,
        pool: PgPool,
        transport: Arc<dyn ImpactTransport>,
        consumptions: Arc<Vec<Consumption>>,
        keyed_by: KeyedByFn<K>,
        project: Arc<Pr>,
        reconcile_keys: Option<ReconcileKeysFn<K>>,
        leader: Option<MirrorGate>,
        reconcile_deadline: Duration,
    ) -> Self {
        Self {
            name,
            nats,
            pool,
            transport,
            consumptions,
            keyed_by,
            project,
            reconcile_keys,
            leader,
            reconcile_deadline,
            shadows: Arc::new(RwLock::new(Shadows::new())),
            read_revision: Arc::new(RwLock::new(Revisions::new())),
            last_seen: Arc::new(RwLock::new(Revisions::new())),
        }
    }

    pub(super) fn reconcile(self: Arc<Self>) -> MirrorRun {
        Box::pin(async move { self.boot_reconcile().await })
    }

    pub(super) fn watch(self: Arc<Self>) -> MirrorRun {
        Box::pin(async move { self.watch_forever().await })
    }

    async fn boot_reconcile(&self) -> Result<(), EngineError> {
        let Read {
            shadows,
            changes,
            read_revision,
        } = self.full_read().await?;
        let touched = self.touched_for(&shadows, &changes).await?;
        *self.shadows.write().await = shadows;
        *self.read_revision.write().await = read_revision.clone();
        *self.last_seen.write().await = read_revision.clone();
        self.converge_as_leader_or_wait(touched, &read_revision)
            .await
    }

    async fn watch_forever(&self) -> Result<(), EngineError> {
        if self.read_revision.read().await.is_empty() {
            self.boot_reconcile().await?;
        }
        loop {
            let resume = self.read_revision.read().await.clone();
            let mut streams = Vec::new();
            for consumption in self.consumptions.iter() {
                let from = match resume.get(consumption.bucket) {
                    Some(snapshot) if snapshot.revision > 0 => snapshot.revision.saturating_add(1),
                    _ => 0,
                };
                streams.push((consumption.open_watch)(self.nats.clone(), from).await?);
            }
            // async-nats watch_all() is future-only. For a truly empty boundary,
            // establish watches first and then check whether the bucket gained
            // data in the scan/watch gap. If so, rescan and reopen from the now
            // nonzero boundary; never leave gap writes behind a future-only watch.
            let mut zero_gap = false;
            for (bucket, snapshot) in &resume {
                if snapshot.revision == 0 {
                    let current =
                        watermark::snapshot(&self.nats, self.name.as_str(), bucket).await?;
                    self.validate_current(bucket, &current).await?;
                    zero_gap |= current.revision > 0;
                }
            }
            if zero_gap {
                self.periodic_reconcile().await?;
                continue;
            }
            let mut merged = futures_util::stream::select_all(streams);
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
                        // Discard events buffered by the old watch. A new scan can
                        // be newer than queued deletes even with bucket history 1.
                        break;
                    }
                    item = merged.next() => {
                        let Some(item) = item else { return Ok(()) };
                        self.on_watch_event(item?).await?;
                    }
                }
            }
        }
    }

    async fn on_watch_event(&self, update: Update) -> Result<(), EngineError> {
        let consumption = self
            .consumptions
            .iter()
            .find(|c| c.prefix == update.change.prefix && c.bucket == update.bucket)
            .expect("registered watch consumption");
        let becomes_empty = {
            let shadows = self.shadows.read().await;
            update.change.is_delete()
                && (consumption.contains)(&shadows, &update.change.key)
                && (consumption.count)(&shadows) == 1
        };
        if becomes_empty && !consumption.allow_empty {
            return Err(empty_prefix(consumption.prefix));
        }
        let current = self
            .read_revision
            .read()
            .await
            .get(consumption.bucket)
            .cloned()
            .expect("watch follows a bucket snapshot");
        let mut touched = {
            let mut shadows = self.shadows.write().await;
            (update.apply)(&mut shadows);
            dedup((self.keyed_by)(&shadows, &update.change))
        };
        if becomes_empty {
            // The deleted payload may have carried the only projection key.
            // Consult persisted keys after removal, against the empty shadow.
            let keys = self
                .reconcile_keys
                .as_ref()
                .expect("optional catalogs validate reconciliation keys at startup");
            touched.extend(keys(self.pool.clone()).await?);
            touched = dedup(touched);
        }
        let mut advance = Revisions::new();
        advance.insert(
            consumption.bucket.to_string(),
            Snapshot {
                created: current.created,
                revision: update.revision,
            },
        );
        merge_forward(&mut *self.last_seen.write().await, &advance);
        self.project_under_lease(touched, &advance).await?;
        Ok(())
    }

    async fn periodic_reconcile(&self) -> Result<(), EngineError> {
        let Read {
            shadows,
            changes,
            read_revision,
        } = self.full_read().await?;
        let touched = self.touched_for(&shadows, &changes).await?;
        *self.shadows.write().await = shadows;
        merge_forward(&mut *self.read_revision.write().await, &read_revision);
        merge_forward(&mut *self.last_seen.write().await, &read_revision);
        self.project_under_lease(touched, &read_revision).await?;
        Ok(())
    }

    async fn validate_current(&self, bucket: &str, current: &Snapshot) -> Result<(), EngineError> {
        let mut conn = self.pool.acquire().await?;
        if let Some(held) = watermark::read(&mut conn, self.name.as_str(), bucket).await? {
            watermark::compatible(self.name.as_str(), bucket, &held, current)?;
        }
        Ok(())
    }

    async fn full_read(&self) -> Result<Read, EngineError> {
        if self.reconcile_keys.is_none() && self.consumptions.iter().any(|c| c.allow_empty) {
            return Err(EngineError::Config(format!(
                "mirror {}: ALLOW_EMPTY requires reconcile_keys",
                self.name
            )));
        }
        let mut read_revision = Revisions::new();
        // Capture every distinct bucket before scanning any consumed prefix.
        for consumption in self.consumptions.iter() {
            if !read_revision.contains_key(consumption.bucket) {
                let snapshot =
                    watermark::snapshot(&self.nats, self.name.as_str(), consumption.bucket).await?;
                self.validate_current(consumption.bucket, &snapshot).await?;
                read_revision.insert(consumption.bucket.to_string(), snapshot);
            }
        }
        let mut shadows = Shadows::new();
        let mut changes = Vec::new();
        for consumption in self.consumptions.iter() {
            let loaded = (consumption.load)(self.nats.clone()).await?;
            if loaded.entries.is_empty() && !consumption.allow_empty {
                return Err(empty_prefix(consumption.prefix));
            }
            // Include every returned entry, even one newer than the boundary S.
            // With history 1, scan/watch overlap is an idempotent current upsert.
            for (key, apply) in loaded.entries {
                changes.push(Change {
                    prefix: consumption.prefix,
                    key,
                    op: super::change::ChangeOp::Put,
                });
                apply(&mut shadows);
            }
        }
        // Refuse a replacement/configuration change during the scan as well.
        for (bucket, held) in &read_revision {
            let current = watermark::snapshot(&self.nats, self.name.as_str(), bucket).await?;
            watermark::compatible(
                self.name.as_str(),
                bucket,
                &watermark::Watermark {
                    stream_created_at: Some(held.created.clone()),
                    revision: held.revision as i64,
                },
                &current,
            )?;
        }
        Ok(Read {
            shadows,
            changes,
            read_revision,
        })
    }

    async fn touched_for(
        &self,
        shadows: &Shadows,
        changes: &[Change],
    ) -> Result<Vec<K>, EngineError> {
        let mut touched = self.join_keys(shadows, changes);
        if let Some(reconcile_keys) = self.reconcile_keys.as_ref() {
            let existing = reconcile_keys(self.pool.clone()).await?;
            touched.extend(existing);
            touched = dedup(touched);
        }
        Ok(touched)
    }

    fn join_keys(&self, shadows: &Shadows, changes: &[Change]) -> Vec<K> {
        let mut all = Vec::new();
        for change in changes {
            all.extend((self.keyed_by)(shadows, change));
        }
        dedup(all)
    }

    fn renew_period(&self) -> Duration {
        self.leader.as_ref().map_or(DEFAULT_BEAT, |gate| gate.beat)
    }

    async fn heartbeat(&self) -> Result<(), EngineError> {
        match tokio::time::timeout(LIVENESS_PROBE_TIMEOUT, self.nats.ping()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(EngineError::Nats(error)),
            Err(_elapsed) => Err(EngineError::Nats(NatsError::Connect(
                "mirror liveness heartbeat timed out; the consumed bucket's broker is unreachable"
                    .to_string(),
            ))),
        }
    }
}

struct Read {
    shadows: Shadows,
    changes: Vec<Change>,
    read_revision: Revisions,
}

fn merge_forward(into: &mut Revisions, from: &Revisions) {
    for (bucket, snapshot) in from {
        let entry = into
            .entry(bucket.clone())
            .or_insert_with(|| snapshot.clone());
        if entry.created == snapshot.created {
            entry.revision = entry.revision.max(snapshot.revision);
        } else {
            *entry = snapshot.clone();
        }
    }
}

fn dedup<K: Clone + Eq + Hash>(keys: Vec<K>) -> Vec<K> {
    let mut seen = HashSet::new();
    keys.into_iter()
        .filter(|key| seen.insert(key.clone()))
        .collect()
}
