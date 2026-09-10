use std::collections::BTreeSet;
use std::error::Error as StdError;

use futures_util::future::BoxFuture;
use sqlx::PgConnection;

use crate::error::EngineError;
use crate::impact::{Dims, ForeignKey, Impact};
use crate::wire::Noun;

use super::consumed::Consumed;
use super::shadow::{Shadow, Shadows};

pub struct Projection<'a> {
    conn: &'a mut PgConnection,
    shadows: &'a Shadows,
    impacts: &'a mut Vec<Impact>,
}

impl<'a> Projection<'a> {
    pub(crate) fn new(
        conn: &'a mut PgConnection,
        shadows: &'a Shadows,
        impacts: &'a mut Vec<Impact>,
    ) -> Self {
        Self {
            conn,
            shadows,
            impacts,
        }
    }

    pub fn conn(&mut self) -> &mut PgConnection {
        self.conn
    }

    pub fn shadow<C: Consumed>(&self) -> Shadow<'_, C> {
        self.shadows.shadow::<C>()
    }

    pub fn impact(&mut self, impact: Impact) {
        self.impacts.push(impact);
    }

    pub fn impact_resource<N: Noun>(
        &mut self,
        key: &N::Key,
        dims: Dims,
    ) -> Result<(), EngineError> {
        self.impacts.push(Impact::resource::<N>(key, dims)?);
        Ok(())
    }

    pub fn impact_foreign(&mut self, namespace: &str, key: &str) -> Result<(), EngineError> {
        self.impacts
            .push(Impact::foreign(ForeignKey::new(namespace, key)?));
        Ok(())
    }

    pub async fn replace_one<R: Known>(&mut self, row: R) -> Result<(), EngineError> {
        row.upsert(self.conn).await?;
        self.impact_foreign(R::NAMESPACE, &row.foreign_key())
    }

    pub async fn remove<S: KnownScope>(&mut self, scope: S) -> Result<(), EngineError> {
        let touched = scope.delete(self.conn).await?;
        for key in touched {
            self.impact_foreign(S::NAMESPACE, &key)?;
        }
        Ok(())
    }

    pub async fn replace<S, R, I>(&mut self, scope: S, rows: I) -> Result<(), EngineError>
    where
        S: KnownScope,
        R: Known,
        I: IntoIterator<Item = R>,
    {
        let mut touched: BTreeSet<String> = scope.delete(self.conn).await?.into_iter().collect();
        for row in rows {
            row.upsert(self.conn).await?;
            touched.insert(row.foreign_key());
        }
        for key in touched {
            self.impact_foreign(R::NAMESPACE, &key)?;
        }
        Ok(())
    }
}

pub trait Known: Send + Sync + 'static {
    const NAMESPACE: &'static str;

    fn foreign_key(&self) -> String;

    fn upsert<'c>(&'c self, conn: &'c mut PgConnection) -> BoxFuture<'c, Result<(), EngineError>>;
}

pub trait KnownScope: Send + Sync {
    const NAMESPACE: &'static str;

    fn delete<'c>(
        &'c self,
        conn: &'c mut PgConnection,
    ) -> BoxFuture<'c, Result<Vec<String>, EngineError>>;
}

pub trait Project<K>: Send + Sync + 'static {
    type Error: StdError + Send + Sync + 'static;

    fn project<'a>(&'a self, cx: Projection<'a>, key: K) -> BoxFuture<'a, Result<(), Self::Error>>;
}
