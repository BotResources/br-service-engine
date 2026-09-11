service_engine::compose_service! {
    principal = crate::kernel::AppPrincipal;
    slice board  ["board"]  { query = board::graphql::BoardQuery,   mutation = board::graphql::BoardMutation,   subscription = board::graphql::BoardSubscription }
    slice card   ["card"]   { query = card::graphql::CardQuery,     mutation = card::graphql::CardMutation,     subscription = card::graphql::CardSubscription }
    slice ledger ["ledger"] { query = ledger::graphql::LedgerQuery, mutation = ledger::graphql::LedgerMutation, subscription = ledger::graphql::LedgerSubscription }
    slice reply  ["reply"]  { query = reply::graphql::ReplyQuery,   mutation = reply::graphql::ReplyMutation,   subscription = reply::graphql::ReplySubscription }
    slice roster ["roster"] { query = roster::graphql::RosterQuery, subscription = roster::graphql::RosterSubscription }
}
