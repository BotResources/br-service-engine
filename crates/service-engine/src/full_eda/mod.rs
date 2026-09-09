use std::hash::Hash;

use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::PgPool;

use crate::error::EngineError;
use crate::name::NounName;

mod erase;
mod store;

pub use erase::erase;
pub use store::FullEda;

pub trait EventSourced: Send + Sync + Sized + 'static {
    type Key: Clone + Eq + Hash + Send + Sync + Serialize + DeserializeOwned + 'static;
    type Event: Serialize + DeserializeOwned + Send + Sync + 'static;
    type Snapshot: Serialize + DeserializeOwned + Send + Sync + 'static;

    const NOUN: NounName;
    const EVENT_VERSION: i32;
    const SNAPSHOT_EVERY: i64 = 1;

    fn key(&self) -> Self::Key;
    fn version(&self) -> i64;
    fn set_version(&mut self, version: i64);

    fn to_snapshot(&self) -> Self::Snapshot;
    fn from_snapshot(snapshot: Self::Snapshot) -> Self;

    fn genesis(&self) -> Self;

    fn apply(&mut self, event: &Self::Event);
    fn check_hydrated(&self) -> Result<(), EngineError>;

    fn upcast(version: i32, payload: &serde_json::Value) -> Result<Self::Event, EngineError>;
}

pub async fn keys<T: EventSourced>(pool: &PgPool) -> Result<Vec<T::Key>, EngineError> {
    store::snapshot_keys::<T>(pool).await
}

pub(crate) fn encode_key_text<K: Serialize>(key: &K) -> Result<String, EngineError> {
    serde_json::to_string(key).map_err(|source| EngineError::Encode {
        what: "full-eda key",
        source,
    })
}

pub(crate) fn decode_key_text<K: DeserializeOwned>(text: &str) -> Result<K, EngineError> {
    serde_json::from_str(text).map_err(|source| EngineError::Decode {
        what: "full-eda key",
        source,
    })
}
