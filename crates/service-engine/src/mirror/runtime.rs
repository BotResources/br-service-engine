use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;

use futures_util::StreamExt;
use sqlx::PgPool;
use tokio::sync::RwLock;

use crate::error::EngineError;
use crate::nats::Nats;
use crate::transport::ImpactTransport;

use super::builder::{Consumption, ReconcileKeysFn};
use super::change::{Change, ChangeOp};
use super::handle::MirrorRun;
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
    shadows: Arc<RwLock<Shadows>>,
}

impl<K, Pr> MirrorRuntime<K, Pr>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    Pr: Project<K>,
{
    pub(super) fn new(
        nats: Nats,
        pool: PgPool,
        transport: Arc<dyn ImpactTransport>,
        consumptions: Arc<Vec<Consumption>>,
        keyed_by: KeyedByFn<K>,
        project: Arc<Pr>,
        reconcile_keys: Option<ReconcileKeysFn<K>>,
    ) -> Self {
        Self {
            nats,
            pool,
            transport,
            consumptions,
            keyed_by,
            project,
            reconcile_keys,
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
        while let Some(item) = merged.next().await {
            let update = item?;
            let touched = {
                let mut shadows = self.shadows.write().await;
                (update.apply)(&mut shadows);
                dedup((self.keyed_by)(&shadows, &update.change))
            };
            let shadows = self.shadows.read().await;
            self.project_all(&shadows, touched).await?;
        }
        Ok(())
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
        let mut impacts = Vec::new();
        for key in keys {
            let cx = Projection::new(&mut tx, shadows, &mut impacts);
            self.project
                .project(cx, key)
                .await
                .map_err(|error| EngineError::Service(Box::new(error)))?;
        }
        if !impacts.is_empty() {
            self.transport.stage_in(&mut tx, &impacts).await?;
        }
        tx.commit().await?;
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
