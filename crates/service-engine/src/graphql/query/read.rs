use async_graphql::Error;
use futures_util::future::BoxFuture;
use sqlx::PgConnection;

use crate::error::EngineError;
use crate::graphql::error::{OrInternal, internal_fault};
use crate::graphql::query::Query;
use crate::persistence::Persistence;
use crate::principal::Principal;
use crate::view::{Projector, ViewKey};

impl<P: Principal> Query<'_, P> {
    pub async fn load_visible<V>(
        &self,
        key: &ViewKey<V>,
    ) -> Result<Option<<V::Store as Persistence>::Aggregate>, Error>
    where
        V: Projector<Principal = P>,
    {
        let loaded = if V::RLS {
            let key = key.clone();
            self.read_under_rls(move |conn| {
                Box::pin(async move { V::Store::load(conn, &key).await })
            })
            .await?
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

    pub async fn read_under_rls<T, F>(&self, read: F) -> Result<T, Error>
    where
        T: Send,
        F: for<'c> FnOnce(&'c mut PgConnection) -> BoxFuture<'c, Result<T, EngineError>> + Send,
    {
        let applier = self.state.runtime().registry().rls().ok_or_else(|| {
            internal_fault("read_under_rls was called but no RlsApplier is registered")
        })?;
        let mut tx = self
            .state
            .pg()
            .begin()
            .await
            .or_internal("begin a read under RLS")?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(&mut *tx)
            .await
            .or_internal("make a read under RLS read-only")?;
        applier
            .apply(&mut tx, self.principal)
            .await
            .or_internal("apply the principal's RLS context to a read")?;
        let outcome = read(&mut tx).await;
        let rolled_back = tx.rollback().await;
        let value = outcome.or_internal("a read under RLS")?;
        rolled_back.or_internal("roll back a read under RLS")?;
        Ok(value)
    }
}
