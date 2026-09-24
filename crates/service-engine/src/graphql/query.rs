use std::sync::Arc;

use async_graphql::{Context, Error, Json};
use serde::de::DeserializeOwned;

use crate::blobs::{BlobRef, Disposition, DownloadUrl};
use crate::dyn_compat::{ErasedPopulation, ErasedProjector};
use crate::error::EngineError;
use crate::graphql::error::{OrInternal, engine_data, internal_fault};
use crate::graphql::state::GraphqlState;
use crate::page::KeyCeiling;
use crate::persistence::{Aggregate, Persistence};
use crate::principal::Principal;
use crate::projector::Projector;
use crate::render::group::Renderer;
use crate::session::WindowParams;
use crate::session::capacity::admit;
use crate::wire::{KeyBytes, ViewBytes};

mod read;

pub use read::Behind;

pub struct Query<'a, P: Principal> {
    state: &'a Arc<GraphqlState<P>>,
    principal: &'a P,
}

impl<'a, P: Principal> Query<'a, P> {
    pub fn new(ctx: &'a Context<'_>) -> Result<Self, Error> {
        Ok(Self {
            state: engine_data::<Arc<GraphqlState<P>>>(ctx)?,
            principal: engine_data::<P>(ctx)?,
        })
    }

    pub async fn fetch<Pr>(&self, key: &Pr::Key) -> Result<Option<Pr::View>, Error>
    where
        Pr: Projector<Principal = P> + Default,
        Pr::View: DeserializeOwned,
    {
        match self.fetch_bytes::<Pr>(key).await? {
            Some(bytes) => Ok(Some(
                bytes
                    .decode::<Pr::View>()
                    .or_internal("decode a query view")?,
            )),
            None => Ok(None),
        }
    }

    pub async fn fetch_window<Pr>(&self, params: WindowParams) -> Result<Vec<Pr::View>, Error>
    where
        Pr: Projector<Principal = P> + Default,
        Pr::View: DeserializeOwned,
    {
        self.fetch_window_bytes::<Pr>(params)
            .await?
            .iter()
            .map(|bytes| {
                bytes
                    .decode::<Pr::View>()
                    .or_internal("decode a query view")
            })
            .collect()
    }

    pub async fn fetch_view<V>(
        &self,
        key: &crate::view::ViewKey<V>,
    ) -> Result<Option<V::Out>, Error>
    where
        V: crate::view::Projector<Principal = P>,
        V::Out: DeserializeOwned,
    {
        self.fetch::<crate::view::ViewProjector<V>>(key).await
    }

    pub async fn fetch_view_window<V>(&self, query: &V::Query) -> Result<Vec<V::Out>, Error>
    where
        V: crate::view::Projector<Principal = P>,
        V::Out: DeserializeOwned,
    {
        let params = WindowParams::encode(query).or_internal("encode a query window")?;
        self.fetch_window::<crate::view::ViewProjector<V>>(params)
            .await
    }

    pub async fn download<V>(
        &self,
        key: &crate::view::ViewKey<V>,
        reference: BlobRef,
        disposition: Disposition,
    ) -> Result<Option<DownloadUrl>, Error>
    where
        V: crate::view::Projector<Principal = P>,
        <V::Store as Persistence>::Aggregate: Aggregate,
    {
        let Some(aggregate) = self.load_visible::<V>(key).await? else {
            return Ok(None);
        };
        if !Aggregate::blob_refs(&aggregate).contains(&reference) {
            return Ok(None);
        }
        match self.state.blob_store() {
            Some(store) => store
                .download_url(self.state.pg(), reference, disposition)
                .await
                .or_internal("sign a blob download"),
            None => Ok(None),
        }
    }

    pub async fn fetch_json<Pr>(
        &self,
        key: &Pr::Key,
    ) -> Result<Option<Json<serde_json::Value>>, Error>
    where
        Pr: Projector<Principal = P> + Default,
    {
        match self.fetch_bytes::<Pr>(key).await? {
            Some(bytes) => Ok(Some(Json(
                view_value(&bytes).or_internal("decode a query view")?,
            ))),
            None => Ok(None),
        }
    }

    pub async fn fetch_window_json<Pr>(
        &self,
        params: WindowParams,
    ) -> Result<Vec<Json<serde_json::Value>>, Error>
    where
        Pr: Projector<Principal = P> + Default,
    {
        self.fetch_window_bytes::<Pr>(params)
            .await?
            .iter()
            .map(|bytes| {
                view_value(bytes)
                    .map(Json)
                    .or_internal("decode a query view")
            })
            .collect()
    }

    async fn fetch_bytes<Pr>(&self, key: &Pr::Key) -> Result<Option<ViewBytes>, Error>
    where
        Pr: Projector<Principal = P> + Default,
    {
        let projector = Pr::default();
        let erased = self.erased(&projector)?;
        let key_bytes = KeyBytes::encode(key).or_internal("encode a query key")?;
        let population = erased
            .populate(
                self.state.pg(),
                &WindowParams::none(),
                KeyCeiling::NONE,
                self.principal,
            )
            .await
            .or_internal("populate a query")?;
        if !is_member(&population, &key_bytes) {
            return Ok(None);
        }
        let mut rendered = self
            .render(
                &erased,
                erased.renders_under_rls(),
                std::slice::from_ref(&key_bytes),
            )
            .await
            .or_internal("render a query")?;
        Ok(rendered.remove(&key_bytes).flatten())
    }

    async fn fetch_window_bytes<Pr>(&self, params: WindowParams) -> Result<Vec<ViewBytes>, Error>
    where
        Pr: Projector<Principal = P> + Default,
    {
        let projector = Pr::default();
        let erased = self.erased(&projector)?;
        let capacity = self.state.runtime().config().window_capacity;
        let population = erased
            .populate(
                self.state.pg(),
                &params,
                KeyCeiling::admission(capacity),
                self.principal,
            )
            .await
            .or_internal("populate a query window")?;
        let keys = member_keys(&population);
        admit(&projector.name(), keys.len(), capacity)?;
        let rendered = self
            .render(&erased, erased.renders_under_rls(), &keys)
            .await
            .or_internal("render a query window")?;
        Ok(keys
            .iter()
            .filter_map(|key| rendered.get(key).cloned().flatten())
            .collect())
    }

    fn erased<Pr>(&self, projector: &Pr) -> Result<Arc<dyn ErasedProjector<P>>, Error>
    where
        Pr: Projector<Principal = P>,
    {
        let name = projector.name();
        self.state
            .runtime()
            .registry()
            .projector(&name)
            .cloned()
            .ok_or_else(|| internal_fault(format_args!("no projector is registered under {name}")))
    }

    async fn render(
        &self,
        erased: &Arc<dyn ErasedProjector<P>>,
        rls: bool,
        keys: &[KeyBytes],
    ) -> Result<crate::render::group::Rendered, EngineError> {
        let runtime = self.state.runtime();
        let renderer = Renderer {
            pg: self.state.pg(),
            chunks: runtime.chunks(),
            rls: runtime.registry().rls(),
            dead_letters: None,
        };
        let cohort = erased.cohort(self.principal);
        let (rendered, _cost) = renderer
            .render(erased, rls, cohort, self.principal, keys)
            .await?;
        Ok(rendered)
    }
}

fn is_member(population: &ErasedPopulation, key: &KeyBytes) -> bool {
    match population {
        ErasedPopulation::Keys(keys) => keys.contains(key),
        ErasedPopulation::Ordered { keys, .. } => keys.contains(key),
        ErasedPopulation::Query(query) => query.authoritative() && query.keys().contains(key),
    }
}

fn member_keys(population: &ErasedPopulation) -> Vec<KeyBytes> {
    match population {
        ErasedPopulation::Keys(keys) => keys.iter().cloned().collect(),
        ErasedPopulation::Ordered { keys, .. } => keys.clone(),
        ErasedPopulation::Query(query) if query.authoritative() => {
            query.keys().iter().cloned().collect()
        }
        ErasedPopulation::Query(_) => Vec::new(),
    }
}

fn view_value(bytes: &ViewBytes) -> Result<serde_json::Value, EngineError> {
    bytes.decode::<serde_json::Value>()
}
