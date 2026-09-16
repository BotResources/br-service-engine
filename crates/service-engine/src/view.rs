use std::any::TypeId;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, OnceLock};

use futures_util::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::PgPool;

use crate::cohort::CohortKey;
use crate::error::EngineError;
use crate::impact::{Dims, ForeignKey, Impact};
use crate::name::{NounName, ProjectorName};
use crate::persistence::{CohortIndex, Persistence};
use crate::population::{Interest, Inverse, Population, WindowQuery};
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

    const RLS: bool = false;

    const RESET_THRESHOLD: Option<usize> = None;

    fn populate(
        cx: &Populate<'_, Self::Principal>,
        query: &Self::Query,
    ) -> impl Future<Output = Result<Population<ViewKey<Self>>, EngineError>> + Send;

    /// Render this row into its view for the principal.
    ///
    /// Return `Err` when the stored row cannot be projected — a nested blob that
    /// will not deserialize, a value the view type cannot represent. The engine
    /// treats that as a poison document: it dead-letters the failure with this
    /// projector as the source and repairs, then ends, the faulted sessions,
    /// rather than letting a panic take the pod down. A total projection that
    /// never fails returns `Ok(view)`.
    fn project(row: &ViewRow<Self>, principal: &Self::Principal) -> Result<Self::Out, EngineError>;

    fn visible(row: &ViewRow<Self>, principal: &Self::Principal) -> bool {
        <Self::Visibility as Visibility>::visible(row, principal)
    }

    fn cohort(principal: &Self::Principal) -> CohortKey {
        CohortKey::principal(principal.id())
    }

    fn emission(_impact: &Impact) -> Emission {
        Emission::Coalesced
    }
}

/// Turn a set of visible keys into the window `Population` **derived** from the
/// view's declared visibility, so the `Keys`-vs-`Query` choice is a property of
/// the declaration rather than a per-`populate` decision that can be got wrong.
///
/// - [`Visibility::LIVE`] `== true` (the default for a cohort view) yields a
///   `Population::Query` seeded with `keys` and carrying an `Interest` on the
///   view's `Noun` and [`Visibility::DEPS`]. That `Interest` is what makes a
///   newly created in-cohort row reach an open session (it would never arrive
///   as a `Keys`/`Fixed` window, since a fresh key is not yet a member) and
///   what makes a membership change repopulate the window.
/// - `LIVE == false` yields a `Population::Keys` (a closed `Fixed` snapshot),
///   the explicit override for a view that is not a live surface.
///
/// A `populate` that returns a `Population` directly bypasses this inference
/// entirely — the other override.
pub fn windowed<V>(keys: BTreeSet<ViewKey<V>>) -> Population<ViewKey<V>>
where
    V: Projector,
    ViewKey<V>: Ord,
{
    if <V::Visibility as Visibility>::LIVE {
        let interest = Interest::new()
            .on_noun(<V::Noun as Noun>::NAME, Dims::EMPTY)
            .on_deps(<V::Visibility as Visibility>::DEPS);
        let query = WindowQuery::new(interest, Arc::new(|_: &ViewKey<V>, _: &Impact| false))
            .with_keys(keys);
        Population::Query(query)
    } else {
        Population::Keys(keys)
    }
}

/// Populate a cohort view by reading **only the caller's rows** through the
/// store's [`CohortIndex`] seam — one indexed query on the cohort column,
/// never a table scan filtered in memory — then derive the window shape with
/// [`windowed`]. This is the read path that principal facts drive: the caller's
/// [`Visibility::memberships`] (rebuilt from the principal's freshly loaded
/// facts) become the cohorts the seam queries.
pub async fn cohort_window<V>(
    cx: &Populate<'_, V::Principal>,
) -> Result<Population<ViewKey<V>>, EngineError>
where
    V: Projector,
    V::Store: CohortIndex,
    ViewKey<V>: Ord,
{
    let cohorts = <V::Visibility as Visibility>::memberships(cx.principal());
    let mut conn = cx.pool().acquire().await.map_err(EngineError::from)?;
    let keys = V::Store::keys_in_cohorts(&mut conn, &cohorts).await?;
    Ok(windowed::<V>(keys.into_iter().collect()))
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

    fn renders_under_rls(&self) -> bool {
        V::RLS
    }

    fn cohort(&self, principal: &V::Principal) -> CohortKey {
        V::cohort(principal)
    }

    fn reset_threshold(&self) -> Option<usize> {
        V::RESET_THRESHOLD
    }

    fn emission(&self, impact: &Impact) -> Emission {
        V::emission(impact)
    }

    fn open_access_reason(&self) -> Option<&'static str> {
        <V::Visibility as crate::visibility::Visibility>::OPEN_ACCESS_REASON
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
    ) -> Result<Option<V::Out>, EngineError> {
        facts
            .rows
            .get(key)
            .filter(|row| V::visible(row, principal))
            .map(|row| V::project(row, principal))
            .transpose()
    }
}
