use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::engine::Engine;
use crate::error::EngineError;
use crate::pipeline::{Bulk, Mutation, MutationExecutor, MutationInput};
use crate::principal::Principal;
use crate::transport::ImpactTransport;

impl<P: Principal> Engine<P> {
    pub fn register_mutation<M, H>(&mut self, handler: H) -> Result<(), EngineError>
    where
        M: MutationInput,
        H: for<'m> Fn(&'m mut Mutation<'m, P>, M) -> BoxFuture<'m, Result<M::Output, M::Error>>
            + Send
            + Sync
            + 'static,
    {
        self.mutations.register::<M, H>(handler)
    }

    pub fn register_bulk<M, H>(&mut self, handler: H) -> Result<(), EngineError>
    where
        M: MutationInput,
        H: for<'m> Fn(&'m mut Bulk<'m, P>, M) -> BoxFuture<'m, Result<M::Output, M::Error>>
            + Send
            + Sync
            + 'static,
    {
        self.mutations.register_bulk::<M, H>(handler)
    }

    pub fn mutation_executor(&self) -> MutationExecutor<P> {
        self.mutations.executor(
            self.pg.clone(),
            self.transport.clone() as Arc<dyn ImpactTransport>,
            self.accumulators.clone(),
            std::sync::Arc::new(self.offers.clone()),
            self.presence.handle(),
            self.blobs.maybe_handle(),
            self.config.lock_timeout,
            self.config.impacts_per_commit,
        )
    }

    pub fn graphql_state(&self) -> crate::graphql::GraphqlState<P> {
        crate::graphql::GraphqlState::new(
            self.mutation_executor(),
            self.render_runtime(),
            self.pg.clone(),
        )
    }
}
