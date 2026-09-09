use std::sync::Arc;

use async_graphql::{MergedObject, MergedSubscription};
use axum::Router;
use br_util_axum_readiness::ReadinessHandle;
use service_engine::{Engine, engine_schema};

use crate::kernel::AppPrincipal;

#[derive(MergedObject, Default)]
pub struct QueryRoot(
    #[cfg(feature = "board")] crate::slices::board::graphql::BoardQuery,
    #[cfg(feature = "card")] crate::slices::card::graphql::CardQuery,
    #[cfg(feature = "ledger")] crate::slices::ledger::graphql::LedgerQuery,
    #[cfg(feature = "reply")] crate::slices::reply::graphql::ReplyQuery,
    #[cfg(feature = "roster")] crate::slices::roster::graphql::RosterQuery,
);

#[derive(MergedObject, Default)]
pub struct MutationRoot(
    #[cfg(feature = "board")] crate::slices::board::graphql::BoardMutation,
    #[cfg(feature = "card")] crate::slices::card::graphql::CardMutation,
    #[cfg(feature = "ledger")] crate::slices::ledger::graphql::LedgerMutation,
    #[cfg(feature = "reply")] crate::slices::reply::graphql::ReplyMutation,
);

#[derive(MergedSubscription, Default)]
pub struct SubscriptionRoot(
    #[cfg(feature = "board")] crate::slices::board::graphql::BoardSubscription,
    #[cfg(feature = "card")] crate::slices::card::graphql::CardSubscription,
    #[cfg(feature = "ledger")] crate::slices::ledger::graphql::LedgerSubscription,
    #[cfg(feature = "reply")] crate::slices::reply::graphql::ReplySubscription,
    #[cfg(feature = "roster")] crate::slices::roster::graphql::RosterSubscription,
);

pub fn build(engine: &mut Engine<AppPrincipal>, readiness: ReadinessHandle) -> Router {
    let state = Arc::new(engine.graphql_state());
    let schema = engine_schema(
        QueryRoot::default(),
        MutationRoot::default(),
        SubscriptionRoot::default(),
        state.clone(),
    );
    engine.set_schema_sdl(schema.sdl());
    service_engine::app(schema, state, readiness)
}
