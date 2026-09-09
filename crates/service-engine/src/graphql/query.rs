use std::sync::Arc;

use async_graphql::{Context, Error, Json};
use serde::de::DeserializeOwned;

use crate::dyn_compat::{ErasedPopulation, ErasedProjector};
use crate::error::EngineError;
use crate::graphql::state::GraphqlState;
use crate::principal::Principal;
use crate::projector::Projector;
use crate::render::group::Renderer;
use crate::session::WindowParams;
use crate::wire::{KeyBytes, ViewBytes};

pub struct Query<'a, P: Principal> {
    state: &'a Arc<GraphqlState<P>>,
    principal: &'a P,
}

impl<'a, P: Principal> Query<'a, P> {
    pub fn new(ctx: &'a Context<'_>) -> Result<Self, Error> {
        Ok(Self {
            state: ctx.data::<Arc<GraphqlState<P>>>()?,
            principal: ctx.data::<P>()?,
        })
    }

    pub async fn fetch<Pr>(&self, key: &Pr::Key) -> Result<Option<Pr::View>, Error>
    where
        Pr: Projector<Principal = P> + Default,
        Pr::View: DeserializeOwned,
    {
        match self.fetch_bytes::<Pr>(key).await? {
            Some(bytes) => Ok(Some(bytes.decode::<Pr::View>().map_err(Error::from)?)),
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
            .map(|bytes| bytes.decode::<Pr::View>().map_err(Error::from))
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
        let params = WindowParams::encode(query)?;
        self.fetch_window::<crate::view::ViewProjector<V>>(params)
            .await
    }

    pub async fn fetch_json<Pr>(
        &self,
        key: &Pr::Key,
    ) -> Result<Option<Json<serde_json::Value>>, Error>
    where
        Pr: Projector<Principal = P> + Default,
    {
        match self.fetch_bytes::<Pr>(key).await? {
            Some(bytes) => Ok(Some(Json(view_value(&bytes)?))),
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
            .map(|bytes| view_value(bytes).map(Json).map_err(Error::from))
            .collect()
    }

    async fn fetch_bytes<Pr>(&self, key: &Pr::Key) -> Result<Option<ViewBytes>, Error>
    where
        Pr: Projector<Principal = P> + Default,
    {
        let projector = Pr::default();
        let erased = self.erased(&projector)?;
        let key_bytes = KeyBytes::encode(key)?;
        let population = erased
            .populate(self.state.pg(), &WindowParams::none(), self.principal)
            .await?;
        if !is_member(&population, &key_bytes) {
            return Ok(None);
        }
        let mut rendered = self
            .render(
                &erased,
                self.rls_engaged(),
                std::slice::from_ref(&key_bytes),
            )
            .await?;
        Ok(rendered.remove(&key_bytes).flatten())
    }

    async fn fetch_window_bytes<Pr>(&self, params: WindowParams) -> Result<Vec<ViewBytes>, Error>
    where
        Pr: Projector<Principal = P> + Default,
    {
        let projector = Pr::default();
        let erased = self.erased(&projector)?;
        let population = erased
            .populate(self.state.pg(), &params, self.principal)
            .await?;
        let keys = member_keys(&population);
        let rendered = self.render(&erased, self.rls_engaged(), &keys).await?;
        Ok(keys
            .iter()
            .filter_map(|key| rendered.get(key).cloned().flatten())
            .collect())
    }

    fn rls_engaged(&self) -> bool {
        self.state.runtime().registry().rls().is_some()
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
            .ok_or_else(|| Error::new(format!("no projector is registered under {name}")))
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
