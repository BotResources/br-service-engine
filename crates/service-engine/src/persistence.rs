use std::hash::Hash;

use futures_util::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::PgConnection;

use crate::blobs::BlobRef;
use crate::error::EngineError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceStyle {
    Crud,
    SoftEda,
    FullEda,
}

pub trait Persistence: Send + Sync + 'static {
    type Aggregate: Send + Sync + 'static;
    type Key: Clone + Eq + Hash + Send + Sync + Serialize + DeserializeOwned + 'static;
    type Event: Send + Sync + 'static;

    const STYLE: PersistenceStyle;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Self::Key,
    ) -> BoxFuture<'a, Result<Option<Self::Aggregate>, EngineError>>;

    fn save<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a Self::Aggregate,
        events: &'a [Self::Event],
    ) -> BoxFuture<'a, Result<(), EngineError>>;

    fn create<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a Self::Aggregate,
        events: &'a [Self::Event],
    ) -> BoxFuture<'a, Result<(), EngineError>>;
}

pub trait Aggregate: Send + Sync + Sized + 'static {
    type Store: Persistence<Aggregate = Self>;

    fn key(&self) -> <Self::Store as Persistence>::Key;

    fn pending_events(&self) -> &[<Self::Store as Persistence>::Event] {
        &[]
    }

    fn blob_refs(&self) -> Vec<BlobRef> {
        Vec::new()
    }
}
