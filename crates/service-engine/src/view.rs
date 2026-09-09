use std::collections::BTreeMap;

use futures_util::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::{PgConnection, PgPool};

use crate::error::EngineError;
use crate::impact::{ForeignKey, Impact};
use crate::name::{NounName, ProjectorName};
use crate::population::{Inverse, Population};
use crate::principal::Principal;
use crate::projector::{Emission, LoadScope, Projector};
use crate::session::WindowParams;
use crate::wire::Noun;

pub struct Populate<'a, P: Principal> {
    pg: &'a PgPool,
    principal: &'a P,
}

impl<'a, P: Principal> Populate<'a, P> {
    pub(crate) fn new(pg: &'a PgPool, principal: &'a P) -> Self {
        Self { pg, principal }
    }

    pub fn pool(&self) -> &PgPool {
        self.pg
    }

    pub fn principal(&self) -> &P {
        self.principal
    }
}

pub type ViewKey<V> = <<V as View>::Noun as Noun>::Key;

pub type RowsFuture<'a, K, R> = BoxFuture<'a, Result<Vec<(K, R)>, EngineError>>;

pub trait View: Send + Sync + 'static {
    type Principal: Principal;
    type Noun: Noun;
    type Row: Send + Sync + 'static;
    type Query: Serialize + DeserializeOwned + Default + Send + Sync + 'static;
    type Out: Clone + PartialEq + Serialize + Send + Sync + 'static;

    const NAME: ProjectorName;

    fn rows<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [ViewKey<Self>],
    ) -> RowsFuture<'a, ViewKey<Self>, Self::Row>;

    fn populate<'a>(
        cx: &'a Populate<'a, Self::Principal>,
        query: &'a Self::Query,
    ) -> BoxFuture<'a, Result<Population<ViewKey<Self>>, EngineError>>;

    fn project(row: &Self::Row, principal: &Self::Principal) -> Self::Out;

    fn emission() -> Emission {
        Emission::Coalesced
    }
}

pub struct ViewFacts<V: View> {
    rows: BTreeMap<ViewKey<V>, V::Row>,
}

pub struct ViewProjector<V: View> {
    nouns: &'static [NounName],
    _marker: std::marker::PhantomData<fn() -> V>,
}

impl<V: View> ViewProjector<V> {
    pub fn new(_view: V) -> Self {
        Self::of()
    }

    pub fn of() -> Self {
        let nouns: &'static [NounName] =
            Box::leak(vec![<V::Noun as Noun>::NAME].into_boxed_slice());
        Self {
            nouns,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<V: View> Default for ViewProjector<V> {
    fn default() -> Self {
        Self::of()
    }
}

impl<V: View> Projector for ViewProjector<V> {
    type Principal = V::Principal;
    type Key = ViewKey<V>;
    type Facts = ViewFacts<V>;
    type View = V::Out;

    fn name(&self) -> ProjectorName {
        V::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        self.nouns
    }

    fn emission(&self, _impact: &Impact) -> Emission {
        V::emission()
    }

    fn populate<'a>(
        &'a self,
        pg: &'a PgPool,
        window: &'a WindowParams,
        principal: &'a V::Principal,
    ) -> BoxFuture<'a, Result<Population<ViewKey<V>>, EngineError>> {
        Box::pin(async move {
            let query = if window.as_value().is_null() {
                V::Query::default()
            } else {
                window.decode::<V::Query>()?
            };
            let cx = Populate::new(pg, principal);
            V::populate(&cx, &query).await
        })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<ViewKey<V>> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, ViewKey<V>, V::Principal>,
    ) -> BoxFuture<'a, Result<ViewFacts<V>, EngineError>> {
        Box::pin(async move {
            let keys = scope.keys().to_vec();
            let rows = match scope {
                LoadScope::Bulk { pg, .. } => {
                    let mut conn = pg.acquire().await.map_err(EngineError::from)?;
                    V::rows(&mut conn, &keys).await?
                }
                LoadScope::PerPrincipal { conn, .. } => V::rows(conn, &keys).await?,
            };
            Ok(ViewFacts {
                rows: rows.into_iter().collect(),
            })
        })
    }

    fn project(
        &self,
        facts: &ViewFacts<V>,
        key: &ViewKey<V>,
        principal: &V::Principal,
    ) -> Option<V::Out> {
        facts.rows.get(key).map(|row| V::project(row, principal))
    }
}
