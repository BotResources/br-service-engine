use std::error::Error as StdError;

use futures_util::future::BoxFuture;
use sqlx::PgConnection;

use crate::error::EngineError;
use crate::impact::{Dims, ForeignKey, Impact};
use crate::wire::Noun;

use super::consumed::Consumed;
use super::known::{self, KnownRow, Written};
use super::row_scope::RowScope;
use super::rows;
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

    pub async fn replace_rows<R, I>(
        &mut self,
        scope: RowScope,
        rows: I,
    ) -> Result<Written, EngineError>
    where
        R: KnownRow,
        I: IntoIterator<Item = R>,
    {
        let changed = rows::replace_rows::<R>(self.conn, scope, rows.into_iter().collect()).await?;
        for key in &changed {
            self.impacts.extend(known::impacts_of::<R>(key)?);
        }
        Ok(if changed.is_empty() {
            Written::Unchanged
        } else {
            Written::Changed
        })
    }
}

pub trait Project<K>: Send + Sync + 'static {
    type Error: StdError + Send + Sync + 'static;

    fn project<'a>(
        &'a self,
        cx: Projection<'a>,
        keys: Vec<K>,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;
}
