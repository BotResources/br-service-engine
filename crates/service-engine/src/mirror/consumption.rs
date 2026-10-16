use std::sync::Arc;

use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use sqlx::PgPool;

use crate::error::EngineError;
use crate::nats::{KvKey, Nats, Seen, Slot};

use super::change::{Change, ChangeOp};
use super::consumed::{Consumed, ConsumedManifest};
use super::rejection::{undecodable_reason, wire_version_reason};
use super::scope::{KeyScope, ScopeRefusal};
use super::shadow::Shadows;

pub(super) type Applier = Box<dyn FnOnce(&mut Shadows) + Send + 'static>;

pub(super) type ReconcileKeysFn<K> =
    Arc<dyn Fn(PgPool) -> BoxFuture<'static, Result<Vec<K>, EngineError>> + Send + Sync>;

pub(super) enum Effect {
    Apply(Applier),
    Rejected { reason: String, retire: Applier },
    Boundary,
}

pub(super) struct Update {
    pub(super) bucket: &'static str,
    pub(super) change: Option<Change>,
    pub(super) effect: Effect,
    pub(super) revision: u64,
}

pub(super) struct Loaded {
    pub(super) entries: Vec<(KvKey, Effect)>,
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
    pub(super) scope: Result<KeyScope, ScopeRefusal>,
    pub(super) expected_manifest: ConsumedManifest,
    pub(super) load: LoadFn,
    pub(super) open_watch: OpenWatchFn,
    pub(super) snapshot: SnapshotFn,
}

pub(super) fn service<E: std::error::Error + Send + Sync + 'static>(error: E) -> EngineError {
    EngineError::Service(Box::new(error))
}

pub(super) fn refused(declared: &str, refusal: &ScopeRefusal) -> EngineError {
    EngineError::Config(format!("consumed {declared:?} {refusal}"))
}

fn scope_of<C: Consumed>() -> Result<KeyScope, EngineError> {
    KeyScope::parse(C::PREFIX).map_err(|refusal| refused(C::PREFIX, &refusal))
}

fn retire<C: Consumed>(key: KvKey, revision: u64) -> Applier {
    Box::new(move |shadows: &mut Shadows| shadows.remove_at::<C>(&key, revision))
}

fn slot_effect<C: Consumed>(key: KvKey, slot: Slot<C>, revision: u64) -> Effect {
    match slot {
        Slot::Value(value) => wire_effect::<C>(key, value, revision),
        Slot::Undecodable(fault) => Effect::Rejected {
            reason: undecodable_reason(&fault),
            retire: retire::<C>(key, revision),
        },
    }
}

fn wire_effect<C: Consumed>(key: KvKey, value: C, revision: u64) -> Effect {
    match C::wire_version(&value) {
        Some(found) if found != C::VERSION => Effect::Rejected {
            reason: wire_version_reason(C::VERSION, found),
            retire: retire::<C>(key, revision),
        },
        _ => Effect::Apply(Box::new(move |shadows: &mut Shadows| {
            shadows.put_at::<C>(key, value, revision)
        })),
    }
}

impl Consumption {
    pub(super) fn of<C: Consumed>() -> Self {
        let load: LoadFn = Arc::new(|nats: Nats| {
            Box::pin(async move {
                let bucket = nats.bind_kv::<C>(C::bucket()).await.map_err(service)?;
                let entries = match scope_of::<C>()? {
                    KeyScope::Prefix { prefix, .. } => {
                        bucket.entries_isolating(&prefix).await.map_err(service)?
                    }
                    KeyScope::Key(key) => bucket
                        .get_isolating(&key)
                        .await
                        .map_err(service)?
                        .map(|(slot, revision)| (key, slot, revision))
                        .into_iter()
                        .collect(),
                };
                let entries = entries
                    .into_iter()
                    .map(|(key, slot, revision)| {
                        let effect = slot_effect::<C>(key.clone(), slot, revision.get());
                        (key, effect)
                    })
                    .collect();
                Ok(Loaded { entries })
            }) as BoxFuture<'static, Result<Loaded, EngineError>>
        });
        let open_watch: OpenWatchFn = Arc::new(|nats: Nats, from: u64| {
            Box::pin(async move {
                let bucket = nats.bind_kv::<C>(C::bucket()).await.map_err(service)?;
                let scope = scope_of::<C>()?;
                let stream = futures_util::stream::unfold(
                    (bucket.watch_all_from(from).await.map_err(service)?, scope),
                    |(mut watch, scope)| async move {
                        match watch.next_seen_where::<C>(|key| scope.matches(key)).await {
                            None => None,
                            Some(Err(error)) => Some((Err(service(error)), (watch, scope))),
                            Some(Ok(watched)) => {
                                Some((Ok(into_update::<C>(watched)), (watch, scope)))
                            }
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
            scope: KeyScope::parse(C::PREFIX),
            expected_manifest: C::manifest(),
            load,
            open_watch,
            snapshot,
        }
    }
}

fn into_update<C: Consumed>(seen: Seen<C>) -> Update {
    match seen {
        Seen::Boundary(revision) => Update {
            bucket: C::bucket(),
            change: None,
            effect: Effect::Boundary,
            revision,
        },
        Seen::Put {
            key,
            slot,
            revision,
        } => {
            let change = Change {
                prefix: C::PREFIX,
                key: key.clone(),
                op: ChangeOp::Put,
            };
            let revision = revision.get();
            let effect = slot_effect::<C>(key, slot, revision);
            Update {
                bucket: C::bucket(),
                change: Some(change),
                effect,
                revision,
            }
        }
        Seen::Delete { key, revision } => {
            let change = Change {
                prefix: C::PREFIX,
                key: key.clone(),
                op: ChangeOp::Delete,
            };
            let revision = revision.get();
            let effect = Effect::Apply(retire::<C>(key, revision));
            Update {
                bucket: C::bucket(),
                change: Some(change),
                effect,
                revision,
            }
        }
    }
}

#[cfg(test)]
mod tests;
