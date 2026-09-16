use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use sqlx::PgPool;

use crate::accumulator::AccumulatorRuntime;
use crate::blobs::BlobHandle;
use crate::error::EngineError;
use crate::offers::OfferStagers;
use crate::pipeline::MutationError;
use crate::pipeline::context::{Bulk, Mutation};
use crate::pipeline::effect::{MutationServices, run_bulk, run_mutation};
use crate::pipeline::policy::PostSavePolicies;
use crate::presence::PresenceHandle;
use crate::principal::Principal;
use crate::transport::ImpactTransport;

pub trait MutationInput: Send + 'static {
    type Output: Send + 'static;
    type Error: crate::pipeline::MutationFault + std::fmt::Display;

    const NAME: &'static str;
}

type ErasedInvoker<P> = Arc<
    dyn Fn(
            MutationServices<P>,
            P,
            Box<dyn Any + Send>,
        ) -> BoxFuture<'static, Result<Box<dyn Any + Send>, MutationError>>
        + Send
        + Sync,
>;

pub struct MutationRegistry<P: Principal> {
    mutations: HashMap<&'static str, ErasedInvoker<P>>,
    bulks: HashMap<&'static str, ErasedInvoker<P>>,
}

impl<P: Principal> Default for MutationRegistry<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: Principal> MutationRegistry<P> {
    pub fn new() -> Self {
        Self {
            mutations: HashMap::new(),
            bulks: HashMap::new(),
        }
    }

    pub fn register<M, H>(&mut self, handler: H) -> Result<(), EngineError>
    where
        M: MutationInput,
        H: for<'m> Fn(&'m mut Mutation<'m, P>, M) -> BoxFuture<'m, Result<M::Output, M::Error>>
            + Send
            + Sync
            + 'static,
    {
        if self.mutations.contains_key(M::NAME) {
            return Err(duplicate("mutation", M::NAME));
        }
        let handler = Arc::new(handler);
        let invoker: ErasedInvoker<P> = Arc::new(move |services, principal, input| {
            let handler = handler.clone();
            Box::pin(async move {
                let input = downcast_input::<M>(input)?;
                let output =
                    run_mutation::<P, M, _>(&services, principal, input, handler.as_ref()).await?;
                Ok(Box::new(output) as Box<dyn Any + Send>)
            })
        });
        self.mutations.insert(M::NAME, invoker);
        Ok(())
    }

    pub fn register_bulk<M, H>(&mut self, handler: H) -> Result<(), EngineError>
    where
        M: MutationInput,
        H: for<'m> Fn(&'m mut Bulk<'m, P>, M) -> BoxFuture<'m, Result<M::Output, M::Error>>
            + Send
            + Sync
            + 'static,
    {
        if self.bulks.contains_key(M::NAME) {
            return Err(duplicate("bulk", M::NAME));
        }
        let handler = Arc::new(handler);
        let invoker: ErasedInvoker<P> = Arc::new(move |services, principal, input| {
            let handler = handler.clone();
            Box::pin(async move {
                let input = downcast_input::<M>(input)?;
                let output =
                    run_bulk::<P, M, _>(&services, principal, input, handler.as_ref()).await?;
                Ok(Box::new(output) as Box<dyn Any + Send>)
            })
        });
        self.bulks.insert(M::NAME, invoker);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn executor(
        &self,
        pool: PgPool,
        transport: Arc<dyn ImpactTransport>,
        accumulators: Arc<AccumulatorRuntime>,
        offers: Arc<OfferStagers>,
        policies: Arc<PostSavePolicies>,
        presence: PresenceHandle<P>,
        blobs: Option<BlobHandle>,
        lock_timeout: Duration,
        impacts_per_commit: usize,
        service: Option<String>,
    ) -> MutationExecutor<P> {
        MutationExecutor {
            services: MutationServices {
                pool,
                transport,
                accumulators,
                offers,
                policies,
                presence,
                blobs,
                lock_timeout,
                impacts_per_commit,
                service,
            },
            mutations: Arc::new(self.mutations.clone()),
            bulks: Arc::new(self.bulks.clone()),
        }
    }
}

fn duplicate(kind: &str, name: &str) -> EngineError {
    EngineError::Config(format!(
        "a {kind} is already registered under the name {name}"
    ))
}

fn downcast_input<M: MutationInput>(input: Box<dyn Any + Send>) -> Result<M, MutationError> {
    input.downcast::<M>().map(|boxed| *boxed).map_err(|_| {
        MutationError::internal(format!("the input for mutation {} downcast", M::NAME))
    })
}

pub struct MutationExecutor<P: Principal> {
    services: MutationServices<P>,
    mutations: Arc<HashMap<&'static str, ErasedInvoker<P>>>,
    bulks: Arc<HashMap<&'static str, ErasedInvoker<P>>>,
}

impl<P: Principal> MutationExecutor<P> {
    pub async fn run<M: MutationInput>(
        &self,
        principal: P,
        input: M,
    ) -> Result<M::Output, MutationError> {
        run_erased::<P, M>(
            &self.mutations,
            &self.services,
            principal,
            input,
            "mutation",
        )
        .await
    }

    pub async fn run_bulk<M: MutationInput>(
        &self,
        principal: P,
        input: M,
    ) -> Result<M::Output, MutationError> {
        run_erased::<P, M>(&self.bulks, &self.services, principal, input, "bulk").await
    }
}

async fn run_erased<P: Principal, M: MutationInput>(
    invokers: &HashMap<&'static str, ErasedInvoker<P>>,
    services: &MutationServices<P>,
    principal: P,
    input: M,
    kind: &str,
) -> Result<M::Output, MutationError> {
    let Some(invoker) = invokers.get(M::NAME) else {
        return Err(MutationError::internal(format!(
            "no {kind} is registered under the name {}",
            M::NAME
        )));
    };
    let output = invoker(services.clone(), principal, Box::new(input)).await?;
    output
        .downcast::<M::Output>()
        .map(|boxed| *boxed)
        .map_err(|_| {
            MutationError::internal(format!("the output of mutation {} downcast", M::NAME))
        })
}
