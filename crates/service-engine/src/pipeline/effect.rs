use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use sqlx::PgPool;

use crate::accumulator::AccumulatorRuntime;
use crate::blobs::BlobHandle;
use crate::error::EngineError;
use crate::offers::OfferStagers;
use uuid::Uuid;

use crate::pipeline::context::{Bulk, Mutation};
use crate::pipeline::mutation::MutationInput;
use crate::pipeline::ops::Ops;
use crate::pipeline::outbound::OutboundContext;
use crate::pipeline::policy::Policies;
use crate::pipeline::staged::Staged;
use crate::pipeline::tx::{begin_scoped, flush_and_commit};
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
    pub(crate) policies: Arc<Policies>,
    pub(crate) presence: PresenceHandle<P>,
    pub(crate) blobs: Option<BlobHandle>,
    pub(crate) lock_timeout: Duration,
    pub(crate) impacts_per_commit: usize,
    pub(crate) service: Option<String>,
}

impl<P: Principal> Clone for MutationServices<P> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            transport: self.transport.clone(),
            accumulators: self.accumulators.clone(),
            offers: self.offers.clone(),
            policies: self.policies.clone(),
            presence: self.presence.clone(),
            blobs: self.blobs.clone(),
            lock_timeout: self.lock_timeout,
            impacts_per_commit: self.impacts_per_commit,
            service: self.service.clone(),
        }
    }
}

fn internal_engine_failure(context: &'static str, error: EngineError) -> MutationError {
    tracing::error!(
        context,
        cause = %crate::chain::describe(&error),
        "the mutation pipeline aborted on an internal engine error; the client receives \
         INTERNAL while the whole cause chain is kept here"
    );
    MutationError::internal(format!("{context} failed"))
}

fn handler_failure(mutation: &'static str, fault: &impl MutationFault) -> MutationError {
    match internal_cause(fault) {
        Some(cause) => {
            tracing::error!(
                mutation,
                %cause,
                "a mutation handler failed without a reason; the client receives INTERNAL while \
                 the whole cause chain is kept here and in the executor's MutationError"
            );
            MutationError::internal(cause)
        }
        None => MutationError::refused(fault.reason(), fault.to_string()),
    }
}

fn internal_cause(fault: &impl MutationFault) -> Option<String> {
    fault
        .reason()
        .is_none()
        .then(|| crate::chain::describe(fault))
}

fn mutation_outbound<P: Principal>(
    services: &MutationServices<P>,
    principal: &P,
) -> OutboundContext {
    OutboundContext {
        actor: principal.passport().to_actor(),
        correlation_id: Uuid::now_v7(),
        causation_id: None,
        producer: services.service.clone(),
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
        .map_err(|error| internal_engine_failure("begin mutation transaction", error.into()))?;
    let mut staged = Staged::default();
    let mut presence_puts = Vec::new();
    let result = {
        let ops = Ops::new(
            &mut tx,
            &mut staged,
            services.accumulators.as_ref(),
            services.offers.clone(),
            services.policies.clone(),
            services.blobs.as_ref(),
            time::now(),
        )
        .with_outbound(mutation_outbound(services, &principal));
        let mut cx = Mutation::new(ops, &principal, &services.presence, &mut presence_puts);
        handler(&mut cx, input).await
    };
    if let Some(refusal) = staged.policy_refusal {
        let _ = tx.rollback().await;
        return Err(MutationError::refused(
            Some(refusal.reason),
            refusal.origin.detail(),
        ));
    }
    let output = match result {
        Ok(output) => output,
        Err(error) => {
            let _ = tx.rollback().await;
            return Err(handler_failure(M::NAME, &error));
        }
    };
    if !staged.is_within(services.impacts_per_commit) {
        let _ = tx.rollback().await;
        return Err(MutationError::fault(format!(
            "the mutation dirtied {} keys, over impacts_per_commit={}; a change this large must \
             go through the bulk path",
            staged.impacts.len(),
            services.impacts_per_commit
        )));
    }
    flush_and_commit(tx, &staged, services.transport.as_ref())
        .await
        .map_err(|error| internal_engine_failure("flush and commit mutation", error))?;
    services
        .accumulators
        .purge_committed_seals(&staged.sealed_keys)
        .await;
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
        .map_err(|error| internal_engine_failure("begin bulk transaction", error.into()))?;
    let mut staged = Staged::default();
    let result = {
        let ops = Ops::new(
            &mut tx,
            &mut staged,
            services.accumulators.as_ref(),
            services.offers.clone(),
            services.policies.clone(),
            services.blobs.as_ref(),
            time::now(),
        )
        .with_outbound(mutation_outbound(services, &principal));
        let mut cx = Bulk::new(ops, &principal);
        handler(&mut cx, input).await
    };
    if let Some(refusal) = staged.policy_refusal {
        let _ = tx.rollback().await;
        return Err(MutationError::refused(
            Some(refusal.reason),
            refusal.origin.detail(),
        ));
    }
    let output = match result {
        Ok(output) => output,
        Err(error) => {
            let _ = tx.rollback().await;
            return Err(handler_failure(M::NAME, &error));
        }
    };
    flush_and_commit(tx, &staged, services.transport.as_ref())
        .await
        .map_err(|error| internal_engine_failure("flush and commit bulk", error))?;
    services
        .accumulators
        .purge_committed_seals(&staged.sealed_keys)
        .await;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::handler_failure;
    use crate::error::EngineError;
    use crate::gate::Reason;
    use crate::pipeline::{MutationError, MutationFault};

    #[derive(Debug, thiserror::Error)]
    enum BoardFault {
        #[error("the board is closed")]
        Closed,
        #[error("loading the board")]
        Load(#[source] EngineError),
    }

    impl MutationFault for BoardFault {
        fn reason(&self) -> Option<Reason> {
            match self {
                Self::Closed => Some(Reason::new("BOARD_CLOSED")),
                Self::Load(_) => None,
            }
        }
    }

    #[test]
    fn a_fault_without_a_reason_fails_internal_with_its_whole_source_chain_as_detail() {
        let fault = BoardFault::Load(EngineError::Config("the board table is absent".into()));

        let failure = handler_failure("board.close", &fault);

        assert_eq!(
            failure,
            MutationError::internal(
                "loading the board: invalid configuration: the board table is absent"
            )
        );
    }

    #[test]
    fn a_refusal_keeps_its_reason_and_its_own_display_as_detail() {
        let failure = handler_failure("board.close", &BoardFault::Closed);

        assert_eq!(
            failure,
            MutationError::refused(Some(Reason::new("BOARD_CLOSED")), "the board is closed")
        );
    }
}
