use std::sync::Arc;

use async_graphql::{Context, Error};
use tokio::sync::OnceCell;

use crate::error::EngineError;
use crate::graphql::error::{UNAUTHENTICATED_CODE, coded_error, engine_data, internal_error};
use crate::graphql::state::GraphqlState;
use crate::principal::Principal;

const REFRESH_CONTEXT: &str = "refresh the principal of a websocket operation";
const REVOKED_MESSAGE: &str = "the principal no longer exists";

#[derive(Debug, Clone, Copy)]
pub(crate) struct SocketOrigin;

pub(crate) struct CurrentPrincipal<'a, P: Principal> {
    stored: &'a P,
    refresh_from: Option<&'a GraphqlState<P>>,
    fresh: OnceCell<P>,
}

impl<'a, P: Principal> CurrentPrincipal<'a, P> {
    pub(crate) fn new(ctx: &'a Context<'_>) -> Result<Self, Error> {
        let state = engine_data::<Arc<GraphqlState<P>>>(ctx)?;
        let stored = engine_data::<P>(ctx)?;
        let refresh_from = ctx.data_opt::<SocketOrigin>().map(|_| Arc::as_ref(state));
        Ok(Self {
            stored,
            refresh_from,
            fresh: OnceCell::new(),
        })
    }

    pub(crate) async fn get(&self) -> Result<&P, Error> {
        let Some(state) = self.refresh_from else {
            return Ok(self.stored);
        };
        self.fresh
            .get_or_try_init(|| refreshed(state, self.stored))
            .await
    }
}

pub(crate) async fn owned<P: Principal>(ctx: &Context<'_>) -> Result<P, Error> {
    CurrentPrincipal::<P>::new(ctx)?.get().await.cloned()
}

async fn refreshed<P: Principal>(state: &GraphqlState<P>, stored: &P) -> Result<P, Error> {
    let registry = state.runtime().registry();
    let resolver = registry
        .resolver()
        .ok_or_else(|| internal_error(REFRESH_CONTEXT, &EngineError::MissingPrincipalResolver))?;
    let mut next = match resolver.resolve(state.pg(), stored).await {
        Ok(Some(next)) => next,
        Ok(None) => return Err(coded_error(UNAUTHENTICATED_CODE, REVOKED_MESSAGE)),
        Err(error) => return Err(internal_error(REFRESH_CONTEXT, &error)),
    };
    registry
        .load_facts(state.pg(), &mut next)
        .await
        .map_err(|error| internal_error(REFRESH_CONTEXT, &error))?;
    Ok(next)
}
