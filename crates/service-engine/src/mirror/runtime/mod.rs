mod lead;
mod watch;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::RwLock;

use crate::config::DEFAULT_BEAT;
use crate::error::EngineError;
use crate::inbound::DeadLetters;
use crate::name::MirrorName;
use crate::nats::{KvBucket, Nats, NatsError};
use crate::transport::ImpactTransport;

const LIVENESS_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

use super::change::{Change, ChangeOp};
use super::consumed::OfferManifest;
use super::consumption::{Consumption, Effect, ReconcileKeysFn};
use super::handle::MirrorRun;
use super::leader::MirrorGate;
use super::projection::Project;
use super::required::RequiredKey;
use super::shadow::Shadows;
use super::watermark::{self, Mark, Snapshot};

type KeyedByFn<K> = Arc<dyn Fn(&Shadows, &Change) -> Vec<K> + Send + Sync>;
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
    required_keys: Vec<RequiredKey>,
    required_tx: Option<Arc<tokio::sync::watch::Sender<Vec<String>>>>,
    shadows: Arc<RwLock<Shadows>>,
    read_revision: Arc<RwLock<Revisions>>,
    last_seen: Arc<RwLock<Revisions>>,
    rejected_prefixes: Arc<RwLock<BTreeSet<&'static str>>>,
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
        required_keys: Vec<RequiredKey>,
        required_tx: Option<Arc<tokio::sync::watch::Sender<Vec<String>>>>,
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
            required_keys,
            required_tx,
            shadows: Arc::new(RwLock::new(Shadows::new())),
            read_revision: Arc::new(RwLock::new(Revisions::new())),
            last_seen: Arc::new(RwLock::new(Revisions::new())),
            rejected_prefixes: Arc::new(RwLock::new(BTreeSet::new())),
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

    pub(super) async fn absorb_full_read(&self) -> Result<Vec<K>, EngineError> {
        let Read {
            shadows,
            changes,
            read_revision,
            rejected,
        } = self.full_read().await?;
        let touched = self.touched_for(&shadows, &changes).await?;
        *self.shadows.write().await = shadows;
        *self.rejected_prefixes.write().await = rejected;
        merge_forward(&mut *self.read_revision.write().await, &read_revision);
        merge_forward(&mut *self.last_seen.write().await, &read_revision);
        self.publish_required_keys().await;
        Ok(touched)
    }

    pub(super) async fn publish_required_keys(&self) {
        let Some(tx) = &self.required_tx else {
            return;
        };
        let missing = {
            let shadows = self.shadows.read().await;
            self.required_keys
                .iter()
                .filter(|required| !required.is_present(&shadows))
                .map(|required| format!("{}: {}", self.name, required.key()))
                .collect::<Vec<_>>()
        };
        tx.send_if_modified(|held| {
            if *held == missing {
                false
            } else {
                *held = missing;
                true
            }
        });
    }

    async fn full_read(&self) -> Result<Read, EngineError> {
        let mut read_revision = Revisions::new();
        for consumption in self.consumptions.iter() {
            if !read_revision.contains_key(consumption.bucket) {
                let snapshot =
                    watermark::snapshot(&self.nats, self.name.as_str(), consumption.bucket).await?;
                read_revision.insert(consumption.bucket.to_string(), snapshot);
            }
        }
        let manifests = self
            .nats
            .published_language::<OfferManifest>()
            .await
            .map_err(EngineError::Nats)?;
        let mut shadows = Shadows::new();
        let mut changes = Vec::new();
        let mut rejected = BTreeSet::new();
        for consumption in self.consumptions.iter() {
            if let Some(reason) = self.manifest_rejection(consumption, &manifests).await? {
                self.dead_letter(consumption.prefix, &reason).await;
                rejected.insert(consumption.prefix);
                continue;
            }
            let loaded = (consumption.load)(self.nats.clone()).await?;
            for (key, effect) in loaded.entries {
                match effect {
                    Effect::Apply(apply) => {
                        changes.push(Change {
                            prefix: consumption.prefix,
                            key,
                            op: ChangeOp::Put,
                        });
                        apply(&mut shadows);
                    }
                    Effect::WireVersionRejected {
                        expected, found, ..
                    } => {
                        self.dead_letter(key.as_str(), &wire_version_reason(expected, found))
                            .await;
                    }
                    Effect::Boundary => {}
                }
            }
        }
        Ok(Read {
            shadows,
            changes,
            read_revision,
            rejected,
        })
    }

    async fn manifest_rejection(
        &self,
        consumption: &Consumption,
        manifests: &KvBucket<OfferManifest>,
    ) -> Result<Option<String>, EngineError> {
        let Some(found) = manifests
            .get(&consumption.manifest_key)
            .await
            .map_err(EngineError::Nats)?
        else {
            return Ok(None);
        };
        match consumption
            .expected_manifest
            .accepts(&found.prefix, found.version)
        {
            Ok(()) => Ok(None),
            Err(mismatch) => Ok(Some(mismatch.to_string())),
        }
    }

    async fn dead_letter(&self, subject: &str, error: &str) {
        let dead_letters =
            DeadLetters::new(self.pool.clone()).with_transport(self.transport.clone());
        if let Err(error) = dead_letters
            .record_mirror(self.name.as_str(), subject, error)
            .await
        {
            tracing::warn!(
                mirror = %self.name,
                subject,
                %error,
                "recording a mirror dead letter failed",
            );
        }
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
    rejected: BTreeSet<&'static str>,
}

fn wire_version_reason(expected: u16, found: u16) -> String {
    format!(
        "mirror value carries wire version {found}, but this consumer is coded against version \
         {expected}; a breaking change moves the version into a new prefix"
    )
}

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
