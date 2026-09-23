use std::sync::Arc;

use async_graphql::{Context, Error, SimpleObject};

use crate::graphql::error::{engine_data, mutation_error};
use crate::graphql::state::GraphqlState;
use crate::pipeline::MutationInput;
use crate::principal::Principal;

#[derive(SimpleObject, Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutationAck {
    pub success: bool,
}

impl MutationAck {
    pub fn ok() -> Self {
        Self { success: true }
    }
}

pub async fn execute<P, M>(ctx: &Context<'_>, input: M) -> Result<M::Output, Error>
where
    P: Principal,
    M: MutationInput,
{
    let state = engine_data::<Arc<GraphqlState<P>>>(ctx)?;
    let principal = engine_data::<P>(ctx)?.clone();
    state
        .executor()
        .run::<M>(principal, input)
        .await
        .map_err(mutation_error)
}

pub async fn execute_bulk<P, M>(ctx: &Context<'_>, input: M) -> Result<M::Output, Error>
where
    P: Principal,
    M: MutationInput,
{
    let state = engine_data::<Arc<GraphqlState<P>>>(ctx)?;
    let principal = engine_data::<P>(ctx)?.clone();
    state
        .executor()
        .run_bulk::<M>(principal, input)
        .await
        .map_err(mutation_error)
}

pub async fn ack<P, M>(ctx: &Context<'_>, input: M) -> Result<MutationAck, Error>
where
    P: Principal,
    M: MutationInput<Output = ()>,
{
    execute::<P, M>(ctx, input)
        .await
        .map(|()| MutationAck::ok())
}

pub async fn ack_bulk<P, M>(ctx: &Context<'_>, input: M) -> Result<MutationAck, Error>
where
    P: Principal,
    M: MutationInput<Output = ()>,
{
    execute_bulk::<P, M>(ctx, input)
        .await
        .map(|()| MutationAck::ok())
}
