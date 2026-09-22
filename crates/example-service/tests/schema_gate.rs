use async_graphql::{EmptySubscription, Schema};
use example_service::slices::{MutationRoot, QueryRoot, SubscriptionRoot};
use service_engine::graphql::{RootPrefix, SchemaSlices, SliceFragment};

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
        use example_service::slices::roster::{RosterQuery, RosterSubscription};
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

    let prefix = RootPrefix::from_snake("example").expect("`example` is a valid root prefix");
    SchemaSlices::assemble(&fragments(), Some(&prefix))
        .expect("the example slices assemble without a collision")
        .verify(&sdl)
        .expect("every root field and object type the composed schema exposes is claimed");
}
