use std::any::{Any, TypeId};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::OnceCell;

use crate::error::EngineError;
use crate::impact::{Dims, Impact};
use crate::name::ProjectorName;
use crate::nats::{KvBucket, KvKey};
use crate::presence::store::PresenceStore;
use crate::presence::{Presence, PresenceKey};
use crate::principal::Principal;

pub(crate) const REASON_PRESENCE_BUCKET: &str = "presence.bucket";

type LaneEntry<P> = (TypeId, Arc<dyn PresenceLane<P>>);

pub(crate) trait PresenceLane<P: Principal>: Send + Sync + 'static {
    fn projector_name(&self) -> ProjectorName;

    fn ttl(&self) -> Duration;

    fn fold_put(&self, raw: &KvKey, value: &Value) -> Option<Result<Impact, EngineError>>;

    fn fold_delete(&self, raw: &KvKey) -> Option<Result<Impact, EngineError>>;

    fn reseed(&self, entries: &[(KvKey, Value)]) -> Result<(), EngineError>;

    fn as_any(&self) -> &dyn Any;
}

pub(crate) struct Lane<P, Pr: Presence> {
    store: Arc<PresenceStore<PresenceKey<Pr>, Pr::Value>>,
    ttl: Duration,
    _principal: PhantomData<fn() -> P>,
}

impl<P, Pr: Presence> Lane<P, Pr> {
    pub(crate) fn new(
        store: Arc<PresenceStore<PresenceKey<Pr>, Pr::Value>>,
        ttl: Duration,
    ) -> Self {
        Self {
            store,
            ttl,
            _principal: PhantomData,
        }
    }

    pub(crate) async fn present(
        &self,
        bucket: &KvBucket<Value>,
        key: &PresenceKey<Pr>,
        value: &Pr::Value,
    ) -> Result<(), EngineError> {
        let kv_key = Pr::kv_key(key);
        let json = serde_json::to_value(value).map_err(|source| EngineError::Encode {
            what: "presence value",
            source,
        })?;
        bucket
            .put(&kv_key, &json)
            .await
            .map_err(|error| EngineError::Service(Box::new(error)))
    }

    fn decode(value: &Value) -> Result<Pr::Value, EngineError> {
        serde_json::from_value(value.clone()).map_err(|source| EngineError::Decode {
            what: "presence value",
            source,
        })
    }
}

impl<P: Principal, Pr: Presence> PresenceLane<P> for Lane<P, Pr> {
    fn projector_name(&self) -> ProjectorName {
        Pr::NAME
    }

    fn ttl(&self) -> Duration {
        self.ttl
    }

    fn fold_put(&self, raw: &KvKey, value: &Value) -> Option<Result<Impact, EngineError>> {
        let key = Pr::parse_kv_key(raw)?;
        Some((|| {
            let decoded = Self::decode(value)?;
            self.store.put(key.clone(), decoded);
            Impact::resource::<Pr::Noun>(&key, Dims::EMPTY)
        })())
    }

    fn fold_delete(&self, raw: &KvKey) -> Option<Result<Impact, EngineError>> {
        let key = Pr::parse_kv_key(raw)?;
        self.store.remove(&key);
        Some(Impact::resource::<Pr::Noun>(&key, Dims::EMPTY))
    }

    fn reseed(&self, entries: &[(KvKey, Value)]) -> Result<(), EngineError> {
        let mut folded: Vec<(PresenceKey<Pr>, Pr::Value)> = Vec::new();
        for (raw, value) in entries {
            let Some(key) = Pr::parse_kv_key(raw) else {
                continue;
            };
            folded.push((key, Self::decode(value)?));
        }
        self.store.reseed(folded);
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct PresenceRegistry<P: Principal> {
    lanes: Vec<LaneEntry<P>>,
    bucket: Arc<OnceCell<KvBucket<Value>>>,
}

impl<P: Principal> PresenceRegistry<P> {
    pub(crate) fn new() -> Self {
        Self {
            lanes: Vec::new(),
            bucket: Arc::new(OnceCell::new()),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.lanes.is_empty()
    }

    pub(crate) fn register<Pr: Presence>(
        &mut self,
        ttl: Duration,
    ) -> Arc<PresenceStore<PresenceKey<Pr>, Pr::Value>> {
        let store = Arc::new(PresenceStore::new());
        let lane: Arc<dyn PresenceLane<P>> = Arc::new(Lane::<P, Pr>::new(store.clone(), ttl));
        self.lanes.push((TypeId::of::<Pr>(), lane));
        store
    }

    pub(crate) fn lanes(&self) -> Vec<Arc<dyn PresenceLane<P>>> {
        self.lanes.iter().map(|(_, lane)| lane.clone()).collect()
    }

    pub(crate) fn bucket_slot(&self) -> Arc<OnceCell<KvBucket<Value>>> {
        self.bucket.clone()
    }

    pub fn handle(&self) -> PresenceHandle<P> {
        PresenceHandle {
            lanes: Arc::new(self.lanes.clone()),
            bucket: self.bucket.clone(),
        }
    }
}

#[derive(Clone)]
pub struct PresenceHandle<P: Principal> {
    lanes: Arc<Vec<LaneEntry<P>>>,
    bucket: Arc<OnceCell<KvBucket<Value>>>,
}

impl<P: Principal> PresenceHandle<P> {
    pub async fn present<Pr: Presence>(
        &self,
        key: &PresenceKey<Pr>,
        value: &Pr::Value,
    ) -> Result<(), EngineError> {
        let lane = self
            .lanes
            .iter()
            .find(|(type_id, _)| *type_id == TypeId::of::<Pr>())
            .map(|(_, lane)| lane)
            .ok_or(EngineError::NotYet {
                capability: "register_presence",
            })?;
        let lane = lane
            .as_any()
            .downcast_ref::<Lane<P, Pr>>()
            .expect("a presence lane keyed by TypeId always downcasts to its own type");
        let bucket = self.bucket.get().ok_or_else(|| {
            EngineError::Posture(
                "the presence bucket is not bound yet; run() binds EPHEMERAL_* at boot".into(),
            )
        })?;
        lane.present(bucket, key, value).await
    }

    pub async fn purge(&self, keys: &[KvKey]) -> Result<(), EngineError> {
        if keys.is_empty() {
            return Ok(());
        }
        let Some(bucket) = self.bucket.get() else {
            return Ok(());
        };
        for key in keys {
            bucket
                .retract(key)
                .await
                .map_err(|error| EngineError::Service(Box::new(error)))?;
        }
        Ok(())
    }
}
