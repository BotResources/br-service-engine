use std::sync::Arc;

use async_graphql::{Context, Error, Json, SimpleObject, Union};
use futures_util::{Stream, StreamExt};

use crate::delta::{Delta, ErasedView};
use crate::graphql::state::GraphqlState;
use crate::principal::Principal;
use crate::session::{AttachRequest, SessionStream, WindowSpec};
use crate::wire::Cause;

#[derive(SimpleObject, Debug, Clone)]
pub struct ProjectedView {
    pub projector: String,
    pub key: Json<serde_json::Value>,
    pub view: Json<serde_json::Value>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct ResetPayload {
    pub revision: u64,
    pub views: Vec<ProjectedView>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct UpsertPayload {
    pub revision: u64,
    pub view: ProjectedView,
    pub cause: Option<Json<serde_json::Value>>,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct RemovePayload {
    pub revision: u64,
    pub projector: String,
    pub key: Json<serde_json::Value>,
    pub cause: Option<Json<serde_json::Value>>,
}

#[derive(Union, Debug, Clone)]
pub enum EngineDelta {
    Reset(ResetPayload),
    Upsert(UpsertPayload),
    Remove(RemovePayload),
}

pub async fn attach<P: Principal>(
    ctx: &Context<'_>,
    windows: Vec<WindowSpec>,
) -> Result<SessionStream, Error> {
    let state = ctx.data::<Arc<GraphqlState<P>>>()?;
    let principal = ctx.data::<P>()?.clone();
    state
        .runtime()
        .attach(AttachRequest::new(principal, windows))
        .await
        .map_err(|error| Error::new(error.to_string()))
}

pub async fn subscribe<P: Principal>(
    ctx: &Context<'_>,
    windows: Vec<WindowSpec>,
) -> Result<impl Stream<Item = Result<EngineDelta, Error>>, Error> {
    let stream = attach::<P>(ctx, windows).await?;
    Ok(stream.map(|delta| to_engine_delta(&delta)))
}

pub fn to_engine_delta(delta: &Delta) -> Result<EngineDelta, Error> {
    match delta {
        Delta::Reset { views, revision } => {
            let views = views
                .iter()
                .map(projected_view)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(EngineDelta::Reset(ResetPayload {
                revision: revision.get(),
                views,
            }))
        }
        Delta::Upsert {
            view,
            revision,
            cause,
        } => Ok(EngineDelta::Upsert(UpsertPayload {
            revision: revision.get(),
            view: projected_view(view)?,
            cause: decode_cause(cause.as_ref())?,
        })),
        Delta::Remove {
            projector,
            key,
            revision,
            cause,
        } => Ok(EngineDelta::Remove(RemovePayload {
            revision: revision.get(),
            projector: projector.to_string(),
            key: Json(key.decode::<serde_json::Value>()?),
            cause: decode_cause(cause.as_ref())?,
        })),
    }
}

fn projected_view(view: &ErasedView) -> Result<ProjectedView, Error> {
    Ok(ProjectedView {
        projector: view.projector.to_string(),
        key: Json(view.key.decode::<serde_json::Value>()?),
        view: Json(view.view.decode::<serde_json::Value>()?),
    })
}

fn decode_cause(cause: Option<&Cause>) -> Result<Option<Json<serde_json::Value>>, Error> {
    match cause {
        Some(cause) => Ok(Some(Json(cause.decode::<serde_json::Value>()?))),
        None => Ok(None),
    }
}
