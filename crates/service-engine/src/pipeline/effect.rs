use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use sqlx::PgPool;

use crate::accumulator::AccumulatorRuntime;
use crate::pipeline::context::{Bulk, Mutation};
use crate::pipeline::dispatch::set_lock_timeout;
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
    pub(crate) presence: PresenceHandle<P>,
    pub(crate) lock_timeout: Duration,
    pub(crate) impacts_per_commit: usize,
}

impl<P: Principal> Clone for MutationServices<P> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            transport: self.transport.clone(),
            accumulators: self.accumulators.clone(),
            presence: self.presence.clone(),
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
    let mut tx = services
        .pool
        .begin()
        .await
        .map_err(|error| MutationError::internal(error.to_string()))?;
    if let Err(error) = set_lock_timeout(&mut tx, services.lock_timeout).await {
        let _ = tx.rollback().await;
        return Err(MutationError::internal(error.to_string()));
    }
    let mut staged = Staged::default();
    let mut presence_puts = Vec::new();
    let result = {
        let ops = Ops::new(
            &mut tx,
            &mut staged,
            services.accumulators.as_ref(),
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
    if let Err(error) = staged.flush(&mut tx, services.transport.as_ref()).await {
        let _ = tx.rollback().await;
        return Err(MutationError::internal(error.to_string()));
    }
    tx.commit()
        .await
        .map_err(|error| MutationError::internal(error.to_string()))?;
    for put in presence_puts {
        put()
            .await
            .map_err(|error| MutationError::internal(error.to_string()))?;
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
    let mut tx = services
        .pool
        .begin()
        .await
        .map_err(|error| MutationError::internal(error.to_string()))?;
    if let Err(error) = set_lock_timeout(&mut tx, services.lock_timeout).await {
        let _ = tx.rollback().await;
        return Err(MutationError::internal(error.to_string()));
    }
    let mut staged = Staged::default();
    let result = {
        let ops = Ops::new(
            &mut tx,
            &mut staged,
            services.accumulators.as_ref(),
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
    if let Err(error) = staged.flush(&mut tx, services.transport.as_ref()).await {
        let _ = tx.rollback().await;
        return Err(MutationError::internal(error.to_string()));
    }
    tx.commit()
        .await
        .map_err(|error| MutationError::internal(error.to_string()))?;
    Ok(output)
}
