use async_graphql::{EmptyMutation, Schema};

fn check(slice: &str, sdl: String) {
    let path = format!(
        "{}/src/slices/{}/schema.graphql",
        env!("CARGO_MANIFEST_DIR"),
        slice
    );
    if std::env::var("BLESS_SCHEMA_FRAGMENTS").is_ok() {
        std::fs::write(&path, &sdl).expect("write the per-slice SDL golden");
        return;
    }
    let committed = std::fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!("missing per-slice SDL at {path}: {error}; regenerate with BLESS_SCHEMA_FRAGMENTS=1")
    });
    assert_eq!(
        sdl, committed,
        "the composed per-slice SDL drifted from {path}; regenerate with BLESS_SCHEMA_FRAGMENTS=1"
    );
}

#[cfg(feature = "board")]
#[test]
fn board_slice_sdl_matches_its_committed_fragment() {
    use example_service::slices::board::graphql::{BoardMutation, BoardQuery, BoardSubscription};
    check(
        "board",
        Schema::build(BoardQuery, BoardMutation, BoardSubscription)
            .finish()
            .sdl(),
    );
}

#[cfg(feature = "card")]
#[test]
fn card_slice_sdl_matches_its_committed_fragment() {
    use example_service::slices::card::graphql::{CardMutation, CardQuery, CardSubscription};
    check(
        "card",
        Schema::build(
            CardQuery::default(),
            CardMutation::default(),
            CardSubscription::default(),
        )
        .finish()
        .sdl(),
    );
}

#[cfg(feature = "ledger")]
#[test]
fn ledger_slice_sdl_matches_its_committed_fragment() {
    use example_service::slices::ledger::graphql::{
        LedgerMutation, LedgerQuery, LedgerSubscription,
    };
    check(
        "ledger",
        Schema::build(LedgerQuery, LedgerMutation, LedgerSubscription)
            .finish()
            .sdl(),
    );
}

#[cfg(feature = "reply")]
#[test]
fn reply_slice_sdl_matches_its_committed_fragment() {
    use example_service::slices::reply::graphql::{ReplyMutation, ReplyQuery, ReplySubscription};
    check(
        "reply",
        Schema::build(ReplyQuery, ReplyMutation, ReplySubscription)
            .finish()
            .sdl(),
    );
}

#[cfg(feature = "roster")]
#[test]
fn roster_slice_sdl_matches_its_committed_fragment() {
    use example_service::slices::roster::graphql::{RosterQuery, RosterSubscription};
    check(
        "roster",
        Schema::build(RosterQuery, EmptyMutation, RosterSubscription)
            .finish()
            .sdl(),
    );
}
