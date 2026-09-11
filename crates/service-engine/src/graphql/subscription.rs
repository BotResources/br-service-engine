use std::sync::Arc;

use async_graphql::{Context, Error};
use futures_util::Stream;

use crate::graphql::state::GraphqlState;
use crate::lanes::LaneNotice;
use crate::principal::Principal;
use crate::session::{AttachRequest, SessionStream, WindowSpec};

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

pub fn lane_notice_stream<P: Principal>(
    ctx: &Context<'_>,
) -> Result<impl Stream<Item = LaneNotice> + use<P>, Error> {
    let state = ctx.data::<Arc<GraphqlState<P>>>()?;
    let channel = state.runtime().lane_channel();
    Ok(crate::lanes::notices(channel.receiver(), channel.lanes()))
}
