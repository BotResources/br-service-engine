use std::any::TypeId;
use std::sync::Arc;

use async_graphql::extensions::{
    Extension, ExtensionContext, ExtensionFactory, NextPrepareRequest,
};
use async_graphql::{Error, Request, ServerError, ServerResult, async_trait};

use crate::error::EngineError;
use crate::graphql::error::{UNAUTHENTICATED_CODE, coded_error, internal_error};
use crate::graphql::state::GraphqlState;
use crate::principal::Principal;

const REFRESH_CONTEXT: &str = "refresh the principal of a websocket operation";
const REVOKED_MESSAGE: &str = "the principal no longer exists";

pub(crate) struct UpgradePrincipal<P: Principal>(pub(crate) P);

pub(crate) struct FreshPrincipal<P: Principal> {
    state: Arc<GraphqlState<P>>,
}

impl<P: Principal> FreshPrincipal<P> {
    pub(crate) fn new(state: Arc<GraphqlState<P>>) -> Self {
        Self { state }
    }
}

impl<P: Principal> ExtensionFactory for FreshPrincipal<P> {
    fn create(&self) -> Arc<dyn Extension> {
        Arc::new(Self {
            state: self.state.clone(),
        })
    }
}

#[async_trait::async_trait]
impl<P: Principal> Extension for FreshPrincipal<P> {
    async fn prepare_request(
        &self,
        ctx: &ExtensionContext<'_>,
        mut request: Request,
        next: NextPrepareRequest<'_>,
    ) -> ServerResult<Request> {
        let upgrade = ctx
            .session_data
            .get(&TypeId::of::<UpgradePrincipal<P>>())
            .and_then(|stored| stored.downcast_ref::<UpgradePrincipal<P>>());
        if let Some(upgrade) = upgrade {
            let fresh = refreshed(&self.state, &upgrade.0)
                .await
                .map_err(into_server_error)?;
            request.data.insert(fresh);
        }
        next.run(ctx, request).await
    }
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

fn into_server_error(error: Error) -> ServerError {
    let mut server = ServerError::new(error.message, None);
    server.extensions = error.extensions;
    server
}
