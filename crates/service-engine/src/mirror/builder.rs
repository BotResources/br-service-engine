use std::sync::Arc;

use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use sqlx::PgPool;

use crate::error::EngineError;
use crate::name::MirrorName;
use crate::nats::{KvEvent, KvKey, KvPrefix, Nats};
use crate::transport::ImpactTransport;

use super::change::{Change, ChangeOp};
use super::consumed::Consumed;
use super::handle::MirrorHandle;
use super::projection::Project;
use super::runtime::MirrorRuntime;
use super::shadow::Shadows;

pub(super) type Applier = Box<dyn FnOnce(&mut Shadows) + Send + 'static>;

pub(super) struct Update {
    pub(super) change: Change,
    pub(super) apply: Applier,
}

type LoadFn = Arc<
    dyn Fn(Nats) -> BoxFuture<'static, Result<Vec<(KvKey, Applier)>, EngineError>> + Send + Sync,
>;
type OpenWatchFn = Arc<
    dyn Fn(
            Nats,
        ) -> BoxFuture<
            'static,
            Result<BoxStream<'static, Result<Update, EngineError>>, EngineError>,
        > + Send
        + Sync,
>;

pub(super) struct Consumption {
    pub(super) prefix: &'static str,
    pub(super) load: LoadFn,
    pub(super) open_watch: OpenWatchFn,
}

fn service<E: std::error::Error + Send + Sync + 'static>(error: E) -> EngineError {
    EngineError::Service(Box::new(error))
}

impl Consumption {
    fn of<C: Consumed>() -> Self {
        let load: LoadFn = Arc::new(|nats: Nats| {
            Box::pin(async move {
                let bucket = nats.bind_kv::<C>(C::bucket()).await.map_err(service)?;
                let prefix = KvPrefix::new(C::PREFIX).map_err(service)?;
                let entries = bucket.entries(&prefix).await.map_err(service)?;
                let loaded = entries
                    .into_iter()
                    .map(|(key, value)| {
                        let shadow_key = key.clone();
                        let applier: Applier =
                            Box::new(move |shadows: &mut Shadows| shadows.put::<C>(key, value));
                        (shadow_key, applier)
                    })
                    .collect();
                Ok(loaded)
            }) as BoxFuture<'static, Result<Vec<(KvKey, Applier)>, EngineError>>
        });
        let open_watch: OpenWatchFn = Arc::new(|nats: Nats| {
            Box::pin(async move {
                let bucket = nats.bind_kv::<C>(C::bucket()).await.map_err(service)?;
                let watch = bucket.watch_all().await.map_err(service)?;
                let stream = futures_util::stream::unfold(watch, |mut watch| async move {
                    loop {
                        match watch.next::<C>().await {
                            None => return None,
                            Some(Err(error)) => return Some((Err(service(error)), watch)),
                            Some(Ok(event)) => {
                                if let Some(update) = into_update::<C>(event) {
                                    return Some((Ok(update), watch));
                                }
                            }
                        }
                    }
                })
                .boxed();
                Ok(stream)
            })
                as BoxFuture<
                    'static,
                    Result<BoxStream<'static, Result<Update, EngineError>>, EngineError>,
                >
        });
        Self {
            prefix: C::PREFIX,
            load,
            open_watch,
        }
    }
}

fn into_update<C: Consumed>(event: KvEvent<C>) -> Option<Update> {
    let (key, op, value) = match event {
        KvEvent::Put { key, value, .. } => (key, ChangeOp::Put, Some(value)),
        KvEvent::Delete { key, .. } => (key, ChangeOp::Delete, None),
    };
    if !key.as_str().starts_with(C::PREFIX) {
        return None;
    }
    let change = Change {
        prefix: C::PREFIX,
        key: key.clone(),
        op,
    };
    let apply: Applier = match value {
        Some(value) => Box::new(move |shadows: &mut Shadows| shadows.put::<C>(key, value)),
        None => Box::new(move |shadows: &mut Shadows| shadows.remove::<C>(&key)),
    };
    Some(Update { change, apply })
}

pub struct Mirror {
    name: MirrorName,
    consumptions: Vec<Consumption>,
}

impl Mirror {
    pub fn new(name: MirrorName) -> Self {
        Self {
            name,
            consumptions: Vec::new(),
        }
    }

    pub fn consume<C: Consumed>(mut self) -> Self {
        self.consumptions.push(Consumption::of::<C>());
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
            keyed_by: Arc::new(keyed_by),
        }
    }
}

type KeyedByFn<K> = Arc<dyn Fn(&Shadows, &Change) -> Vec<K> + Send + Sync>;

pub struct MirrorKeyed<K> {
    name: MirrorName,
    consumptions: Vec<Consumption>,
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
            keyed_by: self.keyed_by,
            project: Arc::new(project),
        }
    }
}

pub struct MirrorReady<K, Pr: Project<K>> {
    name: MirrorName,
    consumptions: Vec<Consumption>,
    keyed_by: KeyedByFn<K>,
    project: Arc<Pr>,
}

impl<K, Pr: Project<K>> MirrorReady<K, Pr>
where
    K: Clone + Eq + std::hash::Hash + Send + Sync + 'static,
{
    pub fn name(&self) -> &MirrorName {
        &self.name
    }

    pub fn build(
        self,
        nats: Nats,
        pool: PgPool,
        transport: Arc<dyn ImpactTransport>,
    ) -> MirrorHandle {
        let runtime = Arc::new(MirrorRuntime::new(
            nats,
            pool,
            transport,
            Arc::new(self.consumptions),
            self.keyed_by,
            self.project,
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
