mod lead;

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use sqlx::PgPool;
use tokio::sync::RwLock;
use tokio::time::MissedTickBehavior;

use crate::config::DEFAULT_BEAT;
use crate::error::EngineError;
use crate::impact::Impact;
use crate::name::MirrorName;
use crate::nats::{KvKey, Nats, NatsError};
use crate::transport::ImpactTransport;

const LIVENESS_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

use super::builder::{Consumption, ReconcileKeysFn, Update};
use super::change::Change;
use super::empty::empty_prefix;
use super::handle::MirrorRun;
use super::leader::MirrorGate;
use super::projection::{Project, Projection};
use super::shadow::Shadows;

type KeyedByFn<K> = Arc<dyn Fn(&Shadows, &Change) -> Vec<K> + Send + Sync>;
type Revisions = HashMap<String, u64>;

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
        let resume = self.read_revision.read().await.clone();
        let mut streams = Vec::new();
        for consumption in self.consumptions.iter() {
            let from = match resume.get(consumption.bucket).copied() {
                Some(revision) => revision.saturating_add(1),
                None => 0,
            };
            streams.push((consumption.open_watch)(self.nats.clone(), from).await?);
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
                }
                item = merged.next() => {
                    let Some(item) = item else { break };
                    self.on_watch_event(item?).await?;
                }
            }
        }
        Ok(())
    }

    async fn on_watch_event(&self, update: Update) -> Result<(), EngineError> {
        if update.change.is_delete()
            && self
                .would_empty(update.change.prefix, &update.change.key)
                .await
        {
            return Err(empty_prefix(self.prefix_of(update.change.prefix)));
        }
        let bucket = self.bucket_of(update.change.prefix).to_string();
        let revision = update.revision;
        let touched = {
            let mut shadows = self.shadows.write().await;
            (update.apply)(&mut shadows);
            dedup((self.keyed_by)(&shadows, &update.change))
        };
        {
            let mut seen = self.last_seen.write().await;
            let entry = seen.entry(bucket.clone()).or_insert(0);
            *entry = (*entry).max(revision);
        }
        let mut advance = Revisions::new();
        advance.insert(bucket, revision);
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

    async fn would_empty(&self, prefix: &str, key: &KvKey) -> bool {
        let shadows = self.shadows.read().await;
        self.consumptions
            .iter()
            .find(|c| c.prefix == prefix)
            .is_some_and(|c| (c.contains)(&shadows, key) && (c.count)(&shadows) == 1)
    }

    fn prefix_of(&self, prefix: &str) -> &'static str {
        self.consumptions
            .iter()
            .find(|c| c.prefix == prefix)
            .map_or("", |c| c.prefix)
    }

    fn bucket_of(&self, prefix: &str) -> &'static str {
        self.consumptions
            .iter()
            .find(|c| c.prefix == prefix)
            .map_or("", |c| c.bucket)
    }

    async fn full_read(&self) -> Result<Read, EngineError> {
        let mut shadows = Shadows::new();
        let mut changes = Vec::new();
        let mut read_revision = Revisions::new();
        for consumption in self.consumptions.iter() {
            let loaded = (consumption.load)(self.nats.clone()).await?;
            if loaded.entries.is_empty() {
                return Err(empty_prefix(consumption.prefix));
            }
            let entry = read_revision
                .entry(consumption.bucket.to_string())
                .or_insert(0);
            *entry = (*entry).max(loaded.read_revision);
            for (key, apply) in loaded.entries {
                changes.push(Change {
                    prefix: consumption.prefix,
                    key,
                    op: super::change::ChangeOp::Put,
                });
                apply(&mut shadows);
            }
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

    async fn project_into(
        &self,
        conn: &mut sqlx::PgConnection,
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

struct Read {
    shadows: Shadows,
    changes: Vec<Change>,
    read_revision: Revisions,
}

fn merge_forward(into: &mut Revisions, from: &Revisions) {
    for (bucket, revision) in from {
        let entry = into.entry(bucket.clone()).or_insert(0);
        *entry = (*entry).max(*revision);
    }
}

fn dedup<K: Clone + Eq + Hash>(keys: Vec<K>) -> Vec<K> {
    let mut seen = HashSet::new();
    keys.into_iter()
        .filter(|key| seen.insert(key.clone()))
        .collect()
}
