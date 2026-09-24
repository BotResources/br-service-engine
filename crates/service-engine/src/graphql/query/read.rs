use async_graphql::Error;
use futures_util::future::BoxFuture;
use sqlx::{PgConnection, Postgres, Transaction};

use crate::error::EngineError;
use crate::graphql::error::{OrInternal, internal_fault};
use crate::graphql::query::Query;
use crate::persistence::{Persistence, PersistenceExt};
use crate::principal::Principal;
use crate::view::{Projector, ViewKey};

type Row<V> = <<V as Projector>::Store as Persistence>::Aggregate;

impl<'a, P: Principal> Query<'a, P> {
    pub async fn load_visible<V>(&self, key: &ViewKey<V>) -> Result<Option<Row<V>>, Error>
    where
        V: Projector<Principal = P>,
    {
        let loaded = if V::RLS {
            let mut tx = self.begin_read(true).await?;
            let outcome = V::Store::load(&mut tx, key).await;
            finish(tx, outcome).await?
        } else {
            let mut conn = self
                .state
                .pg()
                .acquire()
                .await
                .or_internal("acquire a connection for a gated load")?;
            V::Store::load(&mut conn, key)
                .await
                .or_internal("load an aggregate behind its view's visibility")?
        };
        Ok(loaded.filter(|row| V::visible(row, self.principal)))
    }

    pub fn behind<'q, V>(&'q self, key: &'q ViewKey<V>) -> Behind<'q, 'a, P, V>
    where
        V: Projector<Principal = P>,
    {
        Behind { query: self, key }
    }

    pub async fn read_under_rls<T, F>(&self, read: F) -> Result<T, Error>
    where
        T: Send,
        F: for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, Result<T, EngineError>> + Send,
    {
        let mut tx = self.begin_read(true).await?;
        let outcome = read(&mut tx).await;
        finish(tx, outcome).await
    }

    async fn begin_read(&self, under_rls: bool) -> Result<Transaction<'static, Postgres>, Error> {
        let applier = if under_rls {
            Some(self.state.runtime().registry().rls().ok_or_else(|| {
                internal_fault("a read under RLS was asked for but no RlsApplier is registered")
            })?)
        } else {
            None
        };
        let mut tx = self
            .state
            .pg()
            .begin()
            .await
            .or_internal("begin a gated read")?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .or_internal("make a gated read read-only")?;
        if let Some(applier) = applier {
            applier
                .apply(&mut tx, self.principal)
                .await
                .or_internal("apply the principal's RLS context to a read")?;
        }
        Ok(tx)
    }
}

pub struct Behind<'q, 'a, P: Principal, V: Projector> {
    query: &'q Query<'a, P>,
    key: &'q ViewKey<V>,
}

impl<P: Principal, V: Projector<Principal = P>> Behind<'_, '_, P, V> {
    pub async fn read<T, F>(self, read: F) -> Result<Option<T>, Error>
    where
        T: Send,
        F: for<'c> FnOnce(Row<V>, &'c mut PgConnection) -> BoxFuture<'c, Result<T, EngineError>>
            + Send,
    {
        let mut tx = self.query.begin_read(V::RLS).await?;
        let outcome = match V::Store::load(&mut tx, self.key).await {
            Ok(Some(parent)) if V::visible(&parent, self.query.principal) => {
                read(parent, &mut tx).await.map(Some)
            }
            Ok(_) => Ok(None),
            Err(error) => Err(error),
        };
        finish(tx, outcome).await
    }
}

async fn finish<T>(
    tx: Transaction<'static, Postgres>,
    outcome: Result<T, EngineError>,
) -> Result<T, Error> {
    let rolled_back = tx.rollback().await;
    let value = outcome.or_internal("a gated read")?;
    rolled_back.or_internal("roll back a gated read")?;
    Ok(value)
}
