use std::any::TypeId;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::marker::PhantomData;
use std::sync::{Mutex, OnceLock};

use futures_util::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::PgPool;

use crate::error::EngineError;
use crate::impact::{ForeignKey, Impact};
use crate::name::{NounName, ProjectorName};
use crate::persistence::Persistence;
use crate::population::{Inverse, Population};
use crate::principal::Principal;
use crate::projector::{Emission, LoadScope, Projector as RawProjector};
use crate::session::WindowParams;
use crate::visibility::Visibility;
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

pub type ViewKey<V> = <<V as Projector>::Noun as Noun>::Key;

type ViewRow<V> = <<V as Projector>::Store as Persistence>::Aggregate;

pub trait Projector: Send + Sync + 'static {
    type Principal: Principal;
    type Noun: Noun;
    type Store: Persistence<Key = ViewKey<Self>>;
    type Query: Serialize + DeserializeOwned + Default + Send + Sync + 'static;
    type Out: Clone + PartialEq + Serialize + Send + Sync + 'static;
    type Visibility: Visibility<Row = ViewRow<Self>, Principal = Self::Principal>;

    const NAME: ProjectorName;

    fn populate(
        cx: &Populate<'_, Self::Principal>,
        query: &Self::Query,
    ) -> impl Future<Output = Result<Population<ViewKey<Self>>, EngineError>> + Send;

    fn project(row: &ViewRow<Self>, principal: &Self::Principal) -> Self::Out;

    fn visible(row: &ViewRow<Self>, principal: &Self::Principal) -> bool {
        <Self::Visibility as Visibility>::visible(row, principal)
    }

    fn emission() -> Emission {
        Emission::Coalesced
    }
}

fn cached_nouns<V: Projector>() -> &'static [NounName] {
    static CACHE: OnceLock<Mutex<HashMap<TypeId, &'static [NounName]>>> = OnceLock::new();
    let mut cache = CACHE
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(nouns) = cache.get(&TypeId::of::<V>()).copied() {
        return nouns;
    }
    let nouns: &'static [NounName] = Box::leak(vec![<V::Noun as Noun>::NAME].into_boxed_slice());
    cache.insert(TypeId::of::<V>(), nouns);
    nouns
}

pub struct ViewFacts<V: Projector> {
    rows: BTreeMap<ViewKey<V>, ViewRow<V>>,
}

pub struct ViewProjector<V: Projector>(PhantomData<fn() -> V>);

impl<V: Projector> ViewProjector<V> {
    pub fn new(_view: V) -> Self {
        Self(PhantomData)
    }
}

impl<V: Projector> Default for ViewProjector<V> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<V: Projector> RawProjector for ViewProjector<V> {
    type Principal = V::Principal;
    type Key = ViewKey<V>;
    type Facts = ViewFacts<V>;
    type View = V::Out;

    fn name(&self) -> ProjectorName {
        V::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        cached_nouns::<V>()
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
                    V::Store::read_many(&mut conn, &keys).await?
                }
                LoadScope::PerPrincipal { conn, .. } => V::Store::read_many(conn, &keys).await?,
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
        facts
            .rows
            .get(key)
            .filter(|row| V::visible(row, principal))
            .map(|row| V::project(row, principal))
    }
}
