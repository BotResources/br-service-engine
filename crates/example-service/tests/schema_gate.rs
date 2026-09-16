//! The boot-time schema gate, exercised without infrastructure.
//!
//! `Engine::run` assembles every registered [`SliceFragment`] and verifies the
//! composed SDL against them before serving. This test reproduces that gate
//! purely — compose the same schema, derive the same fragments the slices'
//! `register` functions do, and assert the composed schema is fully claimed —
//! so a regression that leaves a root field or object type unowned is caught
//! here, not only in the infra-backed e2e boot.

use async_graphql::{EmptySubscription, Schema};
use example_service::slices::{MutationRoot, QueryRoot, SubscriptionRoot};
use service_engine::graphql::{SchemaSlices, SliceFragment};

fn fragments() -> Vec<SliceFragment> {
    let mut fragments = Vec::new();

    #[cfg(feature = "board")]
    {
        use example_service::slices::board::graphql::{
            BoardMutation, BoardQuery, BoardSubscription,
        };
        fragments.push(SliceFragment::derive::<
            BoardQuery,
            BoardMutation,
            BoardSubscription,
        >("board"));
    }

    #[cfg(feature = "card")]
    {
        use example_service::slices::card::graphql::{
            CardBoardMutation, CardBoardQuery, CardBoardSubscription, CardItemMutation,
            CardItemQuery,
        };
        // One aggregate, two capability fragments.
        fragments.push(SliceFragment::derive::<
            CardItemQuery,
            CardItemMutation,
            EmptySubscription,
        >("card"));
        fragments.push(SliceFragment::derive::<
            CardBoardQuery,
            CardBoardMutation,
            CardBoardSubscription,
        >("card"));
    }

    #[cfg(feature = "ledger")]
    {
        use example_service::slices::ledger::graphql::{
            LedgerMutation, LedgerQuery, LedgerSubscription,
        };
        fragments.push(SliceFragment::derive::<
            LedgerQuery,
            LedgerMutation,
            LedgerSubscription,
        >("ledger"));
    }

    #[cfg(feature = "reply")]
    {
        use example_service::slices::reply::graphql::{
            ReplyMutation, ReplyQuery, ReplySubscription,
        };
        fragments.push(SliceFragment::derive::<
            ReplyQuery,
            ReplyMutation,
            ReplySubscription,
        >("reply"));
    }

    #[cfg(feature = "roster")]
    {
        use async_graphql::EmptyMutation;
        use example_service::slices::roster::graphql::{RosterQuery, RosterSubscription};
        fragments.push(SliceFragment::derive::<
            RosterQuery,
            EmptyMutation,
            RosterSubscription,
        >("roster"));
    }

    fragments
}

#[test]
fn the_composed_example_schema_is_fully_claimed_by_its_slice_fragments() {
    let sdl = Schema::build(
        QueryRoot::default(),
        MutationRoot::default(),
        SubscriptionRoot::default(),
    )
    .finish()
    .sdl();

    SchemaSlices::assemble(&fragments())
        .expect("the example slices assemble without a collision")
        .verify(&sdl)
        .expect("every root field and object type the composed schema exposes is claimed");
}
