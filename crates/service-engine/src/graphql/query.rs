use std::sync::Arc;

use async_graphql::{Context, Error, Json};
use serde::de::DeserializeOwned;

use crate::erase::{ErasedPopulation, ErasedProjector};
use crate::error::EngineError;
use crate::graphql::state::GraphqlState;
use crate::principal::Principal;
use crate::projector::Projector;
use crate::render::group::Renderer;
use crate::session::WindowParams;
use crate::wire::{KeyBytes, ViewBytes};

pub async fn fetch<P, Pr>(
    ctx: &Context<'_>,
    projector: &Pr,
    key: &Pr::Key,
    rls: bool,
) -> Result<Option<Pr::View>, Error>
where
    P: Principal,
    Pr: Projector<Principal = P>,
    Pr::View: DeserializeOwned,
{
    match fetch_bytes::<P, Pr>(ctx, projector, key, rls).await? {
        Some(bytes) => Ok(Some(decode_view::<Pr>(&bytes)?)),
        None => Ok(None),
    }
}

pub async fn fetch_json<P, Pr>(
    ctx: &Context<'_>,
    projector: &Pr,
    key: &Pr::Key,
    rls: bool,
) -> Result<Option<Json<serde_json::Value>>, Error>
where
    P: Principal,
    Pr: Projector<Principal = P>,
{
    match fetch_bytes::<P, Pr>(ctx, projector, key, rls).await? {
        Some(bytes) => Ok(Some(Json(view_value(&bytes)?))),
        None => Ok(None),
    }
}

pub async fn fetch_window<P, Pr>(
    ctx: &Context<'_>,
    projector: &Pr,
    params: WindowParams,
    rls: bool,
) -> Result<Vec<Pr::View>, Error>
where
    P: Principal,
    Pr: Projector<Principal = P>,
    Pr::View: DeserializeOwned,
{
    let bytes = fetch_window_bytes::<P, Pr>(ctx, projector, params, rls).await?;
    bytes
        .iter()
        .map(|view| decode_view::<Pr>(view).map_err(Error::from))
        .collect()
}

pub async fn fetch_window_json<P, Pr>(
    ctx: &Context<'_>,
    projector: &Pr,
    params: WindowParams,
    rls: bool,
) -> Result<Vec<Json<serde_json::Value>>, Error>
where
    P: Principal,
    Pr: Projector<Principal = P>,
{
    let bytes = fetch_window_bytes::<P, Pr>(ctx, projector, params, rls).await?;
    bytes
        .iter()
        .map(|view| view_value(view).map(Json).map_err(Error::from))
        .collect()
}

async fn fetch_bytes<P, Pr>(
    ctx: &Context<'_>,
    projector: &Pr,
    key: &Pr::Key,
    rls: bool,
) -> Result<Option<ViewBytes>, Error>
where
    P: Principal,
    Pr: Projector<Principal = P>,
{
    let state = ctx.data::<Arc<GraphqlState<P>>>()?;
    let principal = ctx.data::<P>()?;
    let erased = erased_projector::<P, Pr>(state, projector)?;
    let key_bytes = KeyBytes::encode(key)?;
    let population = erased
        .populate(state.pg(), &WindowParams::none(), principal)
        .await?;
    if !is_member(&population, &key_bytes) {
        return Ok(None);
    }
    let mut rendered = render_keys::<P>(
        state,
        &erased,
        principal,
        rls,
        std::slice::from_ref(&key_bytes),
    )
    .await?;
    Ok(rendered.remove(&key_bytes).flatten())
}

async fn fetch_window_bytes<P, Pr>(
    ctx: &Context<'_>,
    projector: &Pr,
    params: WindowParams,
    rls: bool,
) -> Result<Vec<ViewBytes>, Error>
where
    P: Principal,
    Pr: Projector<Principal = P>,
{
    let state = ctx.data::<Arc<GraphqlState<P>>>()?;
    let principal = ctx.data::<P>()?;
    let erased = erased_projector::<P, Pr>(state, projector)?;
    let population = erased.populate(state.pg(), &params, principal).await?;
    let keys = member_keys(&population);
    let rendered = render_keys::<P>(state, &erased, principal, rls, &keys).await?;
    Ok(keys
        .iter()
        .filter_map(|key| rendered.get(key).cloned().flatten())
        .collect())
}

fn erased_projector<P, Pr>(
    state: &Arc<GraphqlState<P>>,
    projector: &Pr,
) -> Result<Arc<dyn ErasedProjector<P>>, Error>
where
    P: Principal,
    Pr: Projector<Principal = P>,
{
    let name = projector.name();
    state
        .runtime()
        .registry()
        .projector(&name)
        .cloned()
        .ok_or_else(|| Error::new(format!("no projector is registered under {name}")))
}

async fn render_keys<P: Principal>(
    state: &Arc<GraphqlState<P>>,
    erased: &Arc<dyn ErasedProjector<P>>,
    principal: &P,
    rls: bool,
    keys: &[KeyBytes],
) -> Result<crate::render::group::Rendered, EngineError> {
    let runtime = state.runtime();
    let renderer = Renderer {
        pg: state.pg(),
        chunks: runtime.chunks(),
        rls: runtime.registry().rls(),
    };
    let cohort = erased.cohort(principal);
    let (rendered, _cost) = renderer
        .render(erased, rls, cohort, principal, keys)
        .await?;
    Ok(rendered)
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

fn decode_view<Pr: Projector>(bytes: &ViewBytes) -> Result<Pr::View, EngineError>
where
    Pr::View: DeserializeOwned,
{
    bytes.decode::<Pr::View>()
}

fn view_value(bytes: &ViewBytes) -> Result<serde_json::Value, EngineError> {
    bytes.decode::<serde_json::Value>()
}
