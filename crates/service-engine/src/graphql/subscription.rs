use std::sync::Arc;

use async_graphql::{Context, Error};
use futures_util::Stream;

use crate::graphql::error::{attach_error, engine_data};
use crate::graphql::state::GraphqlState;
use crate::lanes::LaneNotice;
use crate::principal::Principal;
use crate::session::{AttachRequest, SessionStream, WindowSpec};

pub async fn attach<P: Principal>(
    ctx: &Context<'_>,
    windows: Vec<WindowSpec>,
) -> Result<SessionStream, Error> {
    let state = engine_data::<Arc<GraphqlState<P>>>(ctx)?;
    let principal = engine_data::<P>(ctx)?.clone();
    state
        .runtime()
        .attach(AttachRequest::new(principal, windows))
        .await
        .map_err(attach_error)
}

pub fn lane_notice_stream<P: Principal>(
    ctx: &Context<'_>,
) -> Result<impl Stream<Item = LaneNotice> + use<P>, Error> {
    let state = engine_data::<Arc<GraphqlState<P>>>(ctx)?;
    let channel = state.runtime().lane_channel();
    Ok(crate::lanes::notices(channel.receiver(), channel.lanes()))
}
