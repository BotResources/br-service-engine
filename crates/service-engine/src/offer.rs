use futures_util::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::PgConnection;

use crate::error::EngineError;
use crate::nats::KvKey;
use crate::persistence::Aggregate;

pub trait Offer: Send + Sync + 'static {
    type Row: Aggregate;
    type Published: Serialize + DeserializeOwned + PartialEq + Clone + Send + Sync + 'static;

    const NAME: &'static str;
    const PREFIX: &'static str;
    const VERSION: u16 = 1;

    fn key(row: &Self::Row) -> Result<KvKey, EngineError>;

    fn publish(row: &Self::Row) -> Option<Self::Published>;

    fn all(conn: &mut PgConnection) -> BoxFuture<'_, Result<Vec<Self::Row>, EngineError>>;
}

pub trait OfferTrigger<O: Offer>: Aggregate {
    fn key_from(&self) -> Result<KvKey, EngineError>;
}
