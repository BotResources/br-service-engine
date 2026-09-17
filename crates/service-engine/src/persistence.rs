use std::hash::Hash;

use futures_util::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::PgConnection;

use crate::blobs::BlobRef;
use crate::cohort::CohortKey;
use crate::error::EngineError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceStyle {
    Crud,
    SoftEda,
    FullEda,
}

pub type RowBatch<'a, K, A> = BoxFuture<'a, Result<Vec<(K, A)>, EngineError>>;

pub trait Persistence: Send + Sync + 'static {
    type Aggregate: Send + Sync + 'static;
    type Key: Clone + Eq + Hash + Send + Sync + Serialize + DeserializeOwned + 'static;
    type Event: Send + Sync + 'static;

    const STYLE: PersistenceStyle;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Self::Key,
    ) -> BoxFuture<'a, Result<Option<Self::Aggregate>, EngineError>>;

    fn lock<'a>(
        _conn: &'a mut PgConnection,
        _key: &'a Self::Key,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Ok(()) })
    }

    fn row_lock<'a>(
        conn: &'a mut PgConnection,
        table: &'static str,
        key: &'a Self::Key,
    ) -> BoxFuture<'a, Result<(), EngineError>>
    where
        Self::Key: sqlx::Type<sqlx::Postgres>,
        for<'q> &'q Self::Key: sqlx::Encode<'q, sqlx::Postgres>,
    {
        Box::pin(async move {
            let sql = format!("SELECT 1 FROM {table} WHERE id = $1 FOR UPDATE");
            sqlx::query(&sql)
                .bind(key)
                .execute(conn)
                .await
                .map_err(EngineError::Db)?;
            Ok(())
        })
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Self::Key],
    ) -> RowBatch<'a, Self::Key, Self::Aggregate> {
        Box::pin(async move {
            let mut rows = Vec::with_capacity(keys.len());
            for key in keys {
                if let Some(aggregate) = Self::load(&mut *conn, key).await? {
                    rows.push((key.clone(), aggregate));
                }
            }
            Ok(rows)
        })
    }

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

    fn delete<'a>(
        _conn: &'a mut PgConnection,
        _key: &'a Self::Key,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async {
            Err(EngineError::DeleteUnsupported {
                store: std::any::type_name::<Self>(),
            })
        })
    }
}

pub trait CohortIndex: Persistence {
    fn keys_in_cohorts<'a>(
        conn: &'a mut PgConnection,
        cohorts: &'a [CohortKey],
    ) -> BoxFuture<'a, Result<Vec<Self::Key>, EngineError>>;
}

pub trait Aggregate: Clone + Send + Sync + Sized + 'static {
    type Store: Persistence<Aggregate = Self>;

    fn key(&self) -> <Self::Store as Persistence>::Key;

    fn pending_events(&self) -> &[<Self::Store as Persistence>::Event] {
        &[]
    }

    fn blob_refs(&self) -> Vec<BlobRef> {
        Vec::new()
    }
}
