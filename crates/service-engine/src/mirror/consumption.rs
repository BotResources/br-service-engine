use std::sync::Arc;

use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use sqlx::PgPool;

use crate::error::EngineError;
use crate::nats::{KvEvent, KvKey, Nats, Watched};

use super::change::{Change, ChangeOp};
use super::consumed::{Consumed, ConsumedManifest};
use super::scope::{KeyScope, ScopeRefusal};
use super::shadow::Shadows;

pub(super) type Applier = Box<dyn FnOnce(&mut Shadows) + Send + 'static>;

pub(super) type ReconcileKeysFn<K> =
    Arc<dyn Fn(PgPool) -> BoxFuture<'static, Result<Vec<K>, EngineError>> + Send + Sync>;

pub(super) enum Effect {
    Apply(Applier),
    WireVersionRejected {
        expected: u16,
        found: u16,
        retire: Applier,
    },
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

fn wire_effect<C: Consumed>(key: KvKey, value: C, revision: u64) -> Effect {
    match C::wire_version(&value) {
        Some(found) if found != C::VERSION => Effect::WireVersionRejected {
            expected: C::VERSION,
            found,
            retire: Box::new(move |shadows: &mut Shadows| shadows.remove_at::<C>(&key, revision)),
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
                    KeyScope::Prefix { prefix, .. } => bucket
                        .entries_with_revisions(&prefix)
                        .await
                        .map_err(service)?,
                    KeyScope::Key(key) => bucket
                        .get_with_revision(&key)
                        .await
                        .map_err(service)?
                        .map(|(value, revision)| (key, value, revision))
                        .into_iter()
                        .collect(),
                };
                let entries = entries
                    .into_iter()
                    .map(|(key, value, revision)| {
                        let effect = wire_effect::<C>(key.clone(), value, revision.get());
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
                        match watch.next_where::<C>(|key| scope.matches(key)).await {
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

fn into_update<C: Consumed>(watched: Watched<C>) -> Update {
    match watched {
        Watched::Boundary(revision) => Update {
            bucket: C::bucket(),
            change: None,
            effect: Effect::Boundary,
            revision,
        },
        Watched::Event(KvEvent::Put {
            key,
            value,
            revision,
        }) => {
            let change = Change {
                prefix: C::PREFIX,
                key: key.clone(),
                op: ChangeOp::Put,
            };
            let revision = revision.get();
            let effect = wire_effect::<C>(key, value, revision);
            Update {
                bucket: C::bucket(),
                change: Some(change),
                effect,
                revision,
            }
        }
        Watched::Event(KvEvent::Delete { key, revision }) => {
            let change = Change {
                prefix: C::PREFIX,
                key: key.clone(),
                op: ChangeOp::Delete,
            };
            let revision = revision.get();
            let effect = Effect::Apply(Box::new(move |shadows: &mut Shadows| {
                shadows.remove_at::<C>(&key, revision)
            }));
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
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Serialize, Deserialize)]
    struct CutMidSegment {
        #[allow(dead_code)]
        v: u32,
    }
    impl Consumed for CutMidSegment {
        const PREFIX: &'static str = "catalog/item_";
    }

    #[derive(Clone, Serialize, Deserialize)]
    struct Single {
        #[allow(dead_code)]
        v: u32,
    }
    impl Consumed for Single {
        const PREFIX: &'static str = "catalog.settings";
    }

    #[derive(Clone, Serialize, Deserialize)]
    struct Dotted {
        #[allow(dead_code)]
        v: u32,
    }
    impl Consumed for Dotted {
        const PREFIX: &'static str = "catalog.item.";
    }

    #[test]
    fn a_consumption_cut_mid_segment_defers_the_error_instead_of_panicking() {
        let consumption = Consumption::of::<CutMidSegment>();
        assert_eq!(consumption.scope, Err(ScopeRefusal::MidSegment('_')));
    }

    #[test]
    fn a_single_key_consumption_carries_no_manifest() {
        let consumption = Consumption::of::<Single>();
        let scope = consumption.scope.expect("a whole key is accepted");
        assert_eq!(scope.manifest(), None);
    }

    #[test]
    fn a_dot_prefix_consumption_carries_its_sibling_manifest() {
        let consumption = Consumption::of::<Dotted>();
        let scope = consumption
            .scope
            .expect("a dot-terminated prefix is accepted");
        assert_eq!(
            scope.manifest().map(KvKey::as_str),
            Some("catalog.item_manifest")
        );
    }
}
