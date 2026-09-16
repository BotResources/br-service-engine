use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use sqlx::PgPool;

use crate::error::EngineError;
use crate::name::MirrorName;
use crate::nats::{KvEvent, KvKey, KvPrefix, Nats};
use crate::transport::ImpactTransport;

use super::change::{Change, ChangeOp};
use super::consumed::{Consumed, ConsumedManifest, is_raw_json};
use super::handle::MirrorHandle;
use super::leader::{MirrorGate, MirrorLeader};
use super::projection::Project;
use super::runtime::MirrorRuntime;
use super::shadow::Shadows;

pub(super) type Applier = Box<dyn FnOnce(&mut Shadows) + Send + 'static>;

pub(super) type ReconcileKeysFn<K> =
    Arc<dyn Fn(PgPool) -> BoxFuture<'static, Result<Vec<K>, EngineError>> + Send + Sync>;

pub(super) struct Update {
    pub(super) bucket: &'static str,
    pub(super) change: Change,
    pub(super) apply: Applier,
    pub(super) revision: u64,
}

pub(super) struct Loaded {
    pub(super) entries: Vec<(KvKey, Applier)>,
}

type LoadFn = Arc<dyn Fn(Nats) -> BoxFuture<'static, Result<Loaded, EngineError>> + Send + Sync>;
type OpenWatchFn = Arc<
    dyn Fn(
            Nats,
            u64,
        ) -> BoxFuture<
            'static,
            Result<BoxStream<'static, Result<Update, EngineError>>, EngineError>,
        > + Send
        + Sync,
>;
type SnapshotFn = Arc<dyn Fn(&Shadows) -> Vec<Change> + Send + Sync>;

pub(super) struct Consumption {
    pub(super) prefix: &'static str,
    pub(super) bucket: &'static str,
    pub(super) load: LoadFn,
    pub(super) open_watch: OpenWatchFn,
    pub(super) snapshot: SnapshotFn,
}

fn service<E: std::error::Error + Send + Sync + 'static>(error: E) -> EngineError {
    EngineError::Service(Box::new(error))
}

/// What a `.consume::<C>()` records for the engine to check at registration: the
/// manifest the consumer expects, and whether the value is the raw
/// `serde_json::Value` escape. It carries no closures, so reading it settles the
/// typed-consumption law before any watch is opened.
#[derive(Debug, Clone)]
pub struct ConsumedGuard {
    prefix: &'static str,
    manifest: ConsumedManifest,
    is_raw_json: bool,
    escape: bool,
}

impl ConsumedGuard {
    fn of<C: Consumed>() -> Self {
        Self {
            prefix: C::PREFIX,
            manifest: C::manifest(),
            is_raw_json: is_raw_json::<C>(),
            escape: C::RAW_JSON_ESCAPE_HATCH,
        }
    }

    pub fn prefix(&self) -> &'static str {
        self.prefix
    }

    pub fn manifest(&self) -> ConsumedManifest {
        self.manifest
    }

    /// A consumption of the raw `serde_json::Value` without the explicit escape
    /// hatch is refused: the whole value must be typed.
    pub fn refuses_raw_json(&self) -> bool {
        self.is_raw_json && !self.escape
    }
}

impl Consumption {
    fn of<C: Consumed>() -> Self {
        let load: LoadFn = Arc::new(|nats: Nats| {
            Box::pin(async move {
                let bucket = nats.bind_kv::<C>(C::bucket()).await.map_err(service)?;
                let prefix = KvPrefix::new(C::PREFIX).map_err(service)?;
                let entries = bucket
                    .entries_with_revisions(&prefix)
                    .await
                    .map_err(service)?;
                let entries = entries
                    .into_iter()
                    .map(|(key, value, revision)| {
                        let shadow_key = key.clone();
                        // The revision rides with the value: the watch resumes
                        // at the boundary this scan reached, so the two overlap
                        // and the shadow keeps the newer of the two.
                        let applier: Applier = Box::new(move |shadows: &mut Shadows| {
                            shadows.put_at::<C>(key, value, revision.get());
                        });
                        (shadow_key, applier)
                    })
                    .collect();
                Ok(Loaded { entries })
            }) as BoxFuture<'static, Result<Loaded, EngineError>>
        });
        let open_watch: OpenWatchFn = Arc::new(|nats: Nats, from: u64| {
            Box::pin(async move {
                let bucket = nats.bind_kv::<C>(C::bucket()).await.map_err(service)?;
                let prefix = KvPrefix::new(C::PREFIX).map_err(service)?;
                let watch = bucket.watch_all_from(from).await.map_err(service)?;
                let stream = futures_util::stream::unfold(
                    (watch, prefix),
                    |(mut watch, prefix)| async move {
                        match watch.next_under::<C>(&prefix).await {
                            None => None,
                            Some(Err(error)) => Some((Err(service(error)), (watch, prefix))),
                            Some(Ok(event)) => Some((Ok(into_update::<C>(event)), (watch, prefix))),
                        }
                    },
                )
                .boxed();
                Ok(stream)
            })
                as BoxFuture<
                    'static,
                    Result<BoxStream<'static, Result<Update, EngineError>>, EngineError>,
                >
        });
        let snapshot: SnapshotFn = Arc::new(|shadows: &Shadows| {
            shadows
                .shadow::<C>()
                .iter()
                .map(|(key, _)| Change {
                    prefix: C::PREFIX,
                    key: key.clone(),
                    op: ChangeOp::Put,
                })
                .collect()
        });
        Self {
            prefix: C::PREFIX,
            bucket: C::bucket(),
            load,
            open_watch,
            snapshot,
        }
    }
}

fn into_update<C: Consumed>(event: KvEvent<C>) -> Update {
    let (key, op, value, revision) = match event {
        KvEvent::Put {
            key,
            value,
            revision,
        } => (key, ChangeOp::Put, Some(value), revision),
        KvEvent::Delete { key, revision } => (key, ChangeOp::Delete, None, revision),
    };
    let change = Change {
        prefix: C::PREFIX,
        key: key.clone(),
        op,
    };
    let revision = revision.get();
    let apply: Applier = match value {
        Some(value) => {
            Box::new(move |shadows: &mut Shadows| shadows.put_at::<C>(key, value, revision))
        }
        None => Box::new(move |shadows: &mut Shadows| shadows.remove_at::<C>(&key, revision)),
    };
    Update {
        bucket: C::bucket(),
        change,
        apply,
        revision,
    }
}

pub struct Mirror {
    name: MirrorName,
    consumptions: Vec<Consumption>,
    guards: Vec<ConsumedGuard>,
}

impl Mirror {
    pub fn new(name: MirrorName) -> Self {
        Self {
            name,
            consumptions: Vec::new(),
            guards: Vec::new(),
        }
    }

    pub fn consume<C: Consumed>(mut self) -> Self {
        self.consumptions.push(Consumption::of::<C>());
        self.guards.push(ConsumedGuard::of::<C>());
        self
    }

    pub fn keyed_by<K, F>(self, keyed_by: F) -> MirrorKeyed<K>
    where
        K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
        F: Fn(&Shadows, &Change) -> Vec<K> + Send + Sync + 'static,
    {
        MirrorKeyed {
            name: self.name,
            consumptions: self.consumptions,
            guards: self.guards,
            keyed_by: Arc::new(keyed_by),
        }
    }
}

type KeyedByFn<K> = Arc<dyn Fn(&Shadows, &Change) -> Vec<K> + Send + Sync>;

pub struct MirrorKeyed<K> {
    name: MirrorName,
    consumptions: Vec<Consumption>,
    guards: Vec<ConsumedGuard>,
    keyed_by: KeyedByFn<K>,
}

impl<K> MirrorKeyed<K>
where
    K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
{
    pub fn project<Pr: Project<K>>(self, project: Pr) -> MirrorReady<K, Pr> {
        MirrorReady {
            name: self.name,
            consumptions: self.consumptions,
            guards: self.guards,
            keyed_by: self.keyed_by,
            project: Arc::new(project),
            reconcile_keys: None,
            reconcile_deadline: crate::config::DEFAULT_MIRROR_RECONCILE,
        }
    }
}

pub struct MirrorReady<K, Pr: Project<K>> {
    name: MirrorName,
    consumptions: Vec<Consumption>,
    guards: Vec<ConsumedGuard>,
    keyed_by: KeyedByFn<K>,
    project: Arc<Pr>,
    reconcile_keys: Option<ReconcileKeysFn<K>>,
    reconcile_deadline: Duration,
}

impl<K, Pr: Project<K>> MirrorReady<K, Pr>
where
    K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
{
    pub fn name(&self) -> &MirrorName {
        &self.name
    }

    /// What each `.consume::<C>()` recorded, in declaration order — the typed
    /// consumption law the engine checks before it opens a watch.
    pub fn guards(&self) -> &[ConsumedGuard] {
        &self.guards
    }

    /// Refuse a mirror that consumes the raw `serde_json::Value` without the
    /// explicit escape hatch, so the whole value is always typed. Called by
    /// `register_mirror`; a direct `build*` caller may call it too.
    pub fn validate(&self) -> Result<(), EngineError> {
        for guard in &self.guards {
            if guard.refuses_raw_json() {
                return Err(EngineError::RawJsonConsumption {
                    mirror: self.name.clone(),
                    prefix: guard.prefix(),
                });
            }
        }
        Ok(())
    }

    pub fn reconcile_keys<F>(mut self, reconcile_keys: F) -> Self
    where
        F: Fn(PgPool) -> BoxFuture<'static, Result<Vec<K>, EngineError>> + Send + Sync + 'static,
    {
        self.reconcile_keys = Some(Arc::new(reconcile_keys));
        self
    }

    pub fn with_reconcile_deadline(mut self, reconcile_deadline: Duration) -> Self {
        self.reconcile_deadline = reconcile_deadline;
        self
    }

    pub fn build(
        self,
        nats: Nats,
        pool: PgPool,
        transport: Arc<dyn ImpactTransport>,
    ) -> MirrorHandle {
        self.build_with(nats, pool, transport, None)
    }

    pub fn build_led(
        self,
        nats: Nats,
        pool: PgPool,
        transport: Arc<dyn ImpactTransport>,
        leader: MirrorLeader,
    ) -> MirrorHandle {
        let gate = MirrorGate::new(&self.name, leader);
        self.build_with(nats, pool, transport, Some(gate))
    }

    fn build_with(
        self,
        nats: Nats,
        pool: PgPool,
        transport: Arc<dyn ImpactTransport>,
        leader: Option<MirrorGate>,
    ) -> MirrorHandle {
        let runtime = Arc::new(MirrorRuntime::new(
            self.name.clone(),
            nats,
            pool,
            transport,
            Arc::new(self.consumptions),
            self.keyed_by,
            self.project,
            self.reconcile_keys,
            leader,
            self.reconcile_deadline,
        ));
        let name = self.name.clone();
        let reconcile = {
            let runtime = runtime.clone();
            move || runtime.clone().reconcile()
        };
        let watch = move || runtime.clone().watch();
        MirrorHandle::new(name, reconcile, watch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mirror::projection::Projection;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Serialize, Deserialize)]
    struct Typed {
        #[allow(dead_code)]
        v: u32,
    }
    impl Consumed for Typed {
        const PREFIX: &'static str = "typed/v1/";
    }

    struct NoProject;
    impl Project<()> for NoProject {
        type Error = EngineError;
        fn project<'a>(
            &'a self,
            _cx: Projection<'a>,
            _key: (),
        ) -> BoxFuture<'a, Result<(), EngineError>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn ready<C: Consumed>() -> MirrorReady<(), NoProject> {
        Mirror::new(MirrorName::from_static("guarded"))
            .consume::<C>()
            .keyed_by(|_shadows, _change| Vec::<()>::new())
            .project(NoProject)
    }

    #[test]
    fn a_raw_json_consumption_is_refused_without_the_escape_hatch() {
        let refusal = ready::<serde_json::Value>().validate().unwrap_err();
        assert!(matches!(
            refusal,
            EngineError::RawJsonConsumption {
                prefix: "loose/v1/",
                ..
            }
        ));
    }

    #[test]
    fn a_typed_consumption_validates_and_carries_its_manifest() {
        let ready = ready::<Typed>();
        assert!(ready.validate().is_ok());
        let guard = &ready.guards()[0];
        assert_eq!(guard.prefix(), "typed/v1/");
        assert_eq!(guard.manifest(), Typed::manifest());
    }
}
