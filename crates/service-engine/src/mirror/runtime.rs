use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use sqlx::{PgConnection, PgPool};
use tokio::sync::RwLock;
use tokio::time::MissedTickBehavior;

use crate::config::DEFAULT_BEAT;
use crate::error::EngineError;
use crate::housekeeping::leader::try_advisory_xact_lock;
use crate::impact::Impact;
use crate::nats::Nats;
use crate::transport::ImpactTransport;

use super::builder::{Consumption, ReconcileKeysFn};
use super::change::{Change, ChangeOp};
use super::handle::MirrorRun;
use super::leader::{MirrorGate, hold_lease};
use super::projection::{Project, Projection};
use super::shadow::Shadows;

type KeyedByFn<K> = Arc<dyn Fn(&Shadows, &Change) -> Vec<K> + Send + Sync>;

pub(super) struct MirrorRuntime<K, Pr: Project<K>> {
    nats: Nats,
    pool: PgPool,
    transport: Arc<dyn ImpactTransport>,
    consumptions: Arc<Vec<Consumption>>,
    keyed_by: KeyedByFn<K>,
    project: Arc<Pr>,
    reconcile_keys: Option<ReconcileKeysFn<K>>,
    leader: Option<MirrorGate>,
    shadows: Arc<RwLock<Shadows>>,
}

impl<K, Pr> MirrorRuntime<K, Pr>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    Pr: Project<K>,
{
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        nats: Nats,
        pool: PgPool,
        transport: Arc<dyn ImpactTransport>,
        consumptions: Arc<Vec<Consumption>>,
        keyed_by: KeyedByFn<K>,
        project: Arc<Pr>,
        reconcile_keys: Option<ReconcileKeysFn<K>>,
        leader: Option<MirrorGate>,
    ) -> Self {
        Self {
            nats,
            pool,
            transport,
            consumptions,
            keyed_by,
            project,
            reconcile_keys,
            leader,
            shadows: Arc::new(RwLock::new(Shadows::new())),
        }
    }

    pub(super) fn reconcile(self: Arc<Self>) -> MirrorRun {
        Box::pin(async move { self.reconcile_once().await })
    }

    pub(super) fn watch(self: Arc<Self>) -> MirrorRun {
        Box::pin(async move { self.watch_forever().await })
    }

    async fn reconcile_once(&self) -> Result<(), EngineError> {
        let mut fresh = Shadows::new();
        let mut changes = Vec::new();
        for consumption in self.consumptions.iter() {
            let loaded = (consumption.load)(self.nats.clone()).await?;
            if loaded.is_empty() {
                return Err(EngineError::Service(Box::new(EmptyPrefix {
                    prefix: consumption.prefix,
                })));
            }
            for (key, apply) in loaded {
                changes.push(Change {
                    prefix: consumption.prefix,
                    key,
                    op: ChangeOp::Put,
                });
                apply(&mut fresh);
            }
        }
        let mut touched = self.join_keys(&fresh, &changes);
        if let Some(reconcile_keys) = self.reconcile_keys.as_ref() {
            let existing = reconcile_keys(self.pool.clone()).await?;
            touched.extend(existing);
            touched = dedup(touched);
        }
        self.project_all(&fresh, touched).await?;
        *self.shadows.write().await = fresh;
        Ok(())
    }

    async fn watch_forever(&self) -> Result<(), EngineError> {
        let mut streams = Vec::new();
        for consumption in self.consumptions.iter() {
            streams.push((consumption.open_watch)(self.nats.clone()).await?);
        }
        let mut merged = futures_util::stream::select_all(streams);
        let mut beat = tokio::time::interval(self.renew_period());
        beat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        beat.tick().await;
        let mut was_leader = false;
        loop {
            tokio::select! {
                _ = beat.tick() => {
                    self.on_beat(&mut was_leader).await?;
                }
                item = merged.next() => {
                    let Some(item) = item else { break };
                    let update = item?;
                    let touched = {
                        let mut shadows = self.shadows.write().await;
                        (update.apply)(&mut shadows);
                        dedup((self.keyed_by)(&shadows, &update.change))
                    };
                    let shadows = self.shadows.read().await;
                    self.project_all(&shadows, touched).await?;
                }
            }
        }
        Ok(())
    }

    fn renew_period(&self) -> Duration {
        self.leader.as_ref().map_or(DEFAULT_BEAT, |gate| gate.beat)
    }

    async fn on_beat(&self, was_leader: &mut bool) -> Result<(), EngineError> {
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
        let shadows = self.shadows.read().await;
        let mut changes = Vec::new();
        for consumption in self.consumptions.iter() {
            changes.extend((consumption.snapshot)(&shadows));
        }
        let mut touched = self.join_keys(&shadows, &changes);
        if let Some(reconcile_keys) = self.reconcile_keys.as_ref() {
            let existing = reconcile_keys(self.pool.clone()).await?;
            touched.extend(existing);
            touched = dedup(touched);
        }
        self.project_all(&shadows, touched).await
    }

    fn join_keys(&self, shadows: &Shadows, changes: &[Change]) -> Vec<K> {
        let mut all = Vec::new();
        for change in changes {
            all.extend((self.keyed_by)(shadows, change));
        }
        dedup(all)
    }

    async fn project_all(&self, shadows: &Shadows, keys: Vec<K>) -> Result<(), EngineError> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await?;
        if let Some(gate) = &self.leader {
            if !try_advisory_xact_lock(&mut tx, gate.advisory_key).await? {
                let _ = tx.rollback().await;
                return Ok(());
            }
            if !hold_lease(&mut tx, &gate.slot_name, gate.pod.as_str(), gate.lease).await? {
                let _ = tx.rollback().await;
                return Ok(());
            }
        }
        let mut impacts = Vec::new();
        self.project_into(&mut tx, shadows, keys, &mut impacts)
            .await?;
        if !impacts.is_empty() {
            self.transport.stage_in(&mut tx, &impacts).await?;
        }
        tx.commit().await?;
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

fn dedup<K: Clone + Eq + Hash>(keys: Vec<K>) -> Vec<K> {
    let mut seen = HashSet::new();
    keys.into_iter()
        .filter(|key| seen.insert(key.clone()))
        .collect()
}

#[derive(Debug)]
struct EmptyPrefix {
    prefix: &'static str,
}

impl std::fmt::Display for EmptyPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "consumed prefix {} read empty; readiness stays down and the mirror is kept as it is",
            self.prefix
        )
    }
}

impl std::error::Error for EmptyPrefix {}
