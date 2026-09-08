use std::sync::Arc;

use sqlx::PgPool;

use crate::pipeline::MutationExecutor;
use crate::principal::Principal;
use crate::runtime::SessionRuntime;

pub struct GraphqlState<P: Principal> {
    executor: MutationExecutor<P>,
    runtime: Arc<SessionRuntime<P>>,
    pg: PgPool,
}

impl<P: Principal> GraphqlState<P> {
    pub(crate) fn new(
        executor: MutationExecutor<P>,
        runtime: Arc<SessionRuntime<P>>,
        pg: PgPool,
    ) -> Self {
        Self {
            executor,
            runtime,
            pg,
        }
    }

    pub fn pg(&self) -> &PgPool {
        &self.pg
    }

    pub(crate) fn executor(&self) -> &MutationExecutor<P> {
        &self.executor
    }

    pub(crate) fn runtime(&self) -> &Arc<SessionRuntime<P>> {
        &self.runtime
    }
}

impl<P: Principal> std::fmt::Debug for GraphqlState<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphqlState").finish_non_exhaustive()
    }
}
