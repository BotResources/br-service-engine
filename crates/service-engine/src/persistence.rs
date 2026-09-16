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
}

/// A store that can list the keys of the rows in a set of cohorts with **one
/// indexed query**, so a cohort view's `populate` reads only the caller's rows
/// instead of scanning the table and filtering in memory.
///
/// # The cohort-column rule
///
/// **A cohort key MUST be a stored column on the row** — `project_id`,
/// `org_id`, `is_public`, or a `(row, cohort_key)` index row written when the
/// row is saved. A cohort that would need a per-row lookup (a join, a call into
/// another store) to decide membership is **not** a cohort: it cannot be a
/// `WHERE … = ANY($cohorts)` predicate, and `keys_in_cohorts` would degrade to
/// the table scan this seam exists to avoid.
///
/// The `cohorts` are the caller's memberships
/// ([`crate::visibility::Visibility::memberships`]); a row is returned when at
/// least one of its own cohorts ([`crate::visibility::Visibility::cohorts`]) is
/// among them. Bind the keys with [`CohortKey::as_bytes`] against the column
/// (or index) that holds each row's cohort keys. An empty `cohorts` slice
/// returns no keys.
pub trait CohortIndex: Persistence {
    fn keys_in_cohorts<'a>(
        conn: &'a mut PgConnection,
        cohorts: &'a [CohortKey],
    ) -> BoxFuture<'a, Result<Vec<Self::Key>, EngineError>>;
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
