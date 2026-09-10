use std::sync::Arc;

use async_graphql::{Context, Error};

use crate::graphql::state::GraphqlState;
use crate::principal::Principal;
use crate::runtime::PageReport;
use crate::session::{AttachRequest, SessionId, SessionStream, WindowParams, WindowSpec};
use crate::view::Projector as ViewProjector;

pub async fn attach_with_session<P: Principal>(
    ctx: &Context<'_>,
    session: SessionId,
    windows: Vec<WindowSpec>,
) -> Result<SessionStream, Error> {
    let state = ctx.data::<Arc<GraphqlState<P>>>()?;
    let principal = ctx.data::<P>()?.clone();
    state
        .runtime()
        .attach(AttachRequest::new(principal, windows).with_session(session))
        .await
        .map_err(|error| Error::new(error.to_string()))
}

pub async fn page<P, V>(
    ctx: &Context<'_>,
    session: SessionId,
    cursor: &V::Query,
) -> Result<PageReport, Error>
where
    P: Principal,
    V: ViewProjector<Principal = P>,
{
    let state = ctx.data::<Arc<GraphqlState<P>>>()?;
    let params = WindowParams::encode(cursor)?;
    state
        .runtime()
        .page(session, V::NAME, params)
        .await
        .map_err(|error| Error::new(error.to_string()))
}
