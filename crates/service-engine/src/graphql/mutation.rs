use std::sync::Arc;

use async_graphql::{Context, Error, SimpleObject};

use crate::graphql::error::mutation_error;
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
    let state = ctx.data::<Arc<GraphqlState<P>>>()?;
    let principal = ctx.data::<P>()?.clone();
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
    let state = ctx.data::<Arc<GraphqlState<P>>>()?;
    let principal = ctx.data::<P>()?.clone();
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
