use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use sqlx::PgPool;

use crate::accumulator::AccumulatorRuntime;
use crate::blobs::BlobHandle;
use crate::offers::OfferStagers;
use crate::pipeline::context::{Bulk, Mutation};
use crate::pipeline::dispatch::{begin_scoped, flush_and_commit};
use crate::pipeline::mutation::MutationInput;
use crate::pipeline::ops::Ops;
use crate::pipeline::staged::Staged;
use crate::pipeline::{MutationError, MutationFault};
use crate::presence::PresenceHandle;
use crate::principal::Principal;
use crate::time;
use crate::transport::ImpactTransport;

pub(crate) struct MutationServices<P: Principal> {
    pub(crate) pool: PgPool,
    pub(crate) transport: Arc<dyn ImpactTransport>,
    pub(crate) accumulators: Arc<AccumulatorRuntime>,
    pub(crate) offers: Arc<OfferStagers>,
    pub(crate) presence: PresenceHandle<P>,
    pub(crate) blobs: Option<BlobHandle>,
    pub(crate) lock_timeout: Duration,
    pub(crate) impacts_per_commit: usize,
}

impl<P: Principal> Clone for MutationServices<P> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            transport: self.transport.clone(),
            accumulators: self.accumulators.clone(),
            offers: self.offers.clone(),
            presence: self.presence.clone(),
            blobs: self.blobs.clone(),
            lock_timeout: self.lock_timeout,
            impacts_per_commit: self.impacts_per_commit,
        }
    }
}

pub(crate) async fn run_mutation<P, M, H>(
    services: &MutationServices<P>,
    principal: P,
    input: M,
    handler: &H,
) -> Result<M::Output, MutationError>
where
    P: Principal,
    M: MutationInput,
    H: for<'m> Fn(&'m mut Mutation<'m, P>, M) -> BoxFuture<'m, Result<M::Output, M::Error>>,
{
    let mut tx = begin_scoped(&services.pool, services.lock_timeout)
        .await
        .map_err(|error| MutationError::internal(error.to_string()))?;
    let mut staged = Staged::default();
    let mut presence_puts = Vec::new();
    let result = {
        let ops = Ops::new(
            &mut tx,
            &mut staged,
            services.accumulators.as_ref(),
            services.offers.clone(),
            services.blobs.as_ref(),
            time::now(),
        );
        let mut cx = Mutation::new(ops, &principal, &services.presence, &mut presence_puts);
        handler(&mut cx, input).await
    };
    let output = match result {
        Ok(output) => output,
        Err(error) => {
            let _ = tx.rollback().await;
            return Err(MutationError::refused(error.reason(), error.to_string()));
        }
    };
    if !staged.is_within(services.impacts_per_commit) {
        let _ = tx.rollback().await;
        return Err(MutationError::internal(format!(
            "the mutation dirtied {} keys, over impacts_per_commit={}; a change this large must \
             go through the bulk path",
            staged.impacts.len(),
            services.impacts_per_commit
        )));
    }
    flush_and_commit(tx, &staged, services.transport.as_ref())
        .await
        .map_err(|error| MutationError::internal(error.to_string()))?;
    for put in presence_puts {
        if let Err(error) = put().await {
            tracing::warn!(
                reason = %crate::chain::describe(&error),
                "a presence put failed after the mutation committed; the state change stands \
                 and the next value on the loss-tolerant presence lane corrects it, so the \
                 committed mutation is not reported as failed"
            );
        }
    }
    Ok(output)
}

pub(crate) async fn run_bulk<P, M, H>(
    services: &MutationServices<P>,
    principal: P,
    input: M,
    handler: &H,
) -> Result<M::Output, MutationError>
where
    P: Principal,
    M: MutationInput,
    H: for<'m> Fn(&'m mut Bulk<'m, P>, M) -> BoxFuture<'m, Result<M::Output, M::Error>>,
{
    let mut tx = begin_scoped(&services.pool, services.lock_timeout)
        .await
        .map_err(|error| MutationError::internal(error.to_string()))?;
    let mut staged = Staged::default();
    let result = {
        let ops = Ops::new(
            &mut tx,
            &mut staged,
            services.accumulators.as_ref(),
            services.offers.clone(),
            services.blobs.as_ref(),
            time::now(),
        );
        let mut cx = Bulk::new(ops, &principal);
        handler(&mut cx, input).await
    };
    let output = match result {
        Ok(output) => output,
        Err(error) => {
            let _ = tx.rollback().await;
            return Err(MutationError::refused(error.reason(), error.to_string()));
        }
    };
    flush_and_commit(tx, &staged, services.transport.as_ref())
        .await
        .map_err(|error| MutationError::internal(error.to_string()))?;
    Ok(output)
}
