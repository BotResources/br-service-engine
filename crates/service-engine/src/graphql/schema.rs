use std::sync::Arc;

use async_graphql::{ObjectType, Schema, SubscriptionType};

use crate::graphql::state::GraphqlState;
use crate::principal::Principal;

pub fn engine_schema<P, Q, M, S>(
    query: Q,
    mutation: M,
    subscription: S,
    state: Arc<GraphqlState<P>>,
) -> Schema<Q, M, S>
where
    P: Principal,
    Q: ObjectType + 'static,
    M: ObjectType + 'static,
    S: SubscriptionType + 'static,
{
    Schema::build(query, mutation, subscription)
        .data(state)
        .finish()
}
