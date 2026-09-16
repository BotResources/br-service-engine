mod lead;
mod watch;

use std::collections::{BTreeMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::RwLock;

use crate::config::DEFAULT_BEAT;
use crate::error::EngineError;
use crate::name::MirrorName;
use crate::nats::{Nats, NatsError};
use crate::transport::ImpactTransport;

const LIVENESS_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

use super::builder::{Consumption, ReconcileKeysFn};
use super::change::{Change, ChangeOp};
use super::handle::MirrorRun;
use super::leader::MirrorGate;
use super::projection::Project;
use super::shadow::Shadows;
use super::watermark::{self, Mark, Snapshot};

type KeyedByFn<K> = Arc<dyn Fn(&Shadows, &Change) -> Vec<K> + Send + Sync>;
/// The snapshot a mirror is at, one entry per distinct consumed bucket. Two
/// consumptions of the same prefix in different buckets keep distinct entries.
type Revisions = BTreeMap<String, Snapshot>;

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

    pub(super) async fn boot_reconcile(&self) -> Result<(), EngineError> {
        let touched = self.absorb_full_read().await?;
        self.converge_as_leader_or_wait(touched).await
    }

    pub(super) async fn periodic_reconcile(&self) -> Result<(), EngineError> {
        let touched = self.absorb_full_read().await?;
        let adopt = self.read_revision.read().await.clone();
        self.project_under_lease(touched, &adopt, Mark::Adopt)
            .await?;
        Ok(())
    }

    /// One full read folded into the runtime's own state: the shadows it read,
    /// and both cursors carried onto whatever identity that read saw. Returns
    /// the keys the read touches, for the caller to project under the lease.
    pub(super) async fn absorb_full_read(&self) -> Result<Vec<K>, EngineError> {
        let Read {
            shadows,
            changes,
            read_revision,
        } = self.full_read().await?;
        let touched = self.touched_for(&shadows, &changes).await?;
        *self.shadows.write().await = shadows;
        merge_forward(&mut *self.read_revision.write().await, &read_revision);
        merge_forward(&mut *self.last_seen.write().await, &read_revision);
        Ok(touched)
    }

    /// One full read of every consumed prefix, against a boundary captured
    /// before the first scan. What the read finds is never judged: a prefix that
    /// reads empty is a converged prefix with nothing in it, and the reconcile
    /// below projects `known_*` to empty as it would to any other value. Only a
    /// read that fails leaves the projections and the watermark untouched.
    async fn full_read(&self) -> Result<Read, EngineError> {
        let mut read_revision = Revisions::new();
        for consumption in self.consumptions.iter() {
            if !read_revision.contains_key(consumption.bucket) {
                let snapshot =
                    watermark::snapshot(&self.nats, self.name.as_str(), consumption.bucket).await?;
                read_revision.insert(consumption.bucket.to_string(), snapshot);
            }
        }
        let mut shadows = Shadows::new();
        let mut changes = Vec::new();
        for consumption in self.consumptions.iter() {
            let loaded = (consumption.load)(self.nats.clone()).await?;
            // Every scanned entry is applied, including one whose revision is
            // above the captured S: the watch resumes at S + 1, so the overlap
            // replays a current value the projection absorbs idempotently.
            for (key, apply) in loaded.entries {
                changes.push(Change {
                    prefix: consumption.prefix,
                    key,
                    op: ChangeOp::Put,
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

    /// The keys a full read touches: the ones the snapshot joins to, plus every
    /// key the service has already persisted. Reconciling the persisted keys
    /// against the snapshot is the one behaviour of every scan — it is how a
    /// removal reaches `known_*`, an empty snapshot included.
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

/// Carry each bucket forward: monotonic inside one stream identity, replaced
/// outright when the identity under it changed, since a new stream's sequence
/// means nothing to the old one.
fn merge_forward(into: &mut Revisions, from: &Revisions) {
    for (bucket, snapshot) in from {
        match into.get_mut(bucket) {
            Some(held) if held.created == snapshot.created => {
                held.revision = held.revision.max(snapshot.revision);
            }
            _ => {
                into.insert(bucket.clone(), snapshot.clone());
            }
        }
    }
}

fn dedup<K: Clone + Eq + Hash>(keys: Vec<K>) -> Vec<K> {
    let mut seen = HashSet::new();
    keys.into_iter()
        .filter(|key| seen.insert(key.clone()))
        .collect()
}
