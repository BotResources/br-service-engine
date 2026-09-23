use std::sync::Arc;

use async_graphql::{Context, Error};

use crate::graphql::error::{OrInternal, attach_error, engine_data, page_error};
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
    let state = engine_data::<Arc<GraphqlState<P>>>(ctx)?;
    let principal = engine_data::<P>(ctx)?.clone();
    state
        .runtime()
        .attach(AttachRequest::new(principal, windows).with_session(session))
        .await
        .map_err(attach_error)
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
    let state = engine_data::<Arc<GraphqlState<P>>>(ctx)?;
    let principal = engine_data::<P>(ctx)?.clone();
    let params = WindowParams::encode(cursor).or_internal("encode a page cursor")?;
    state
        .runtime()
        .page(&principal, session, V::NAME, params)
        .await
        .map_err(page_error)
}
