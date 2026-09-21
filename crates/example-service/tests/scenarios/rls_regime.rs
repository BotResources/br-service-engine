use std::time::Duration;

use sqlx::Row;
use uuid::Uuid;

use crate::harness::{Subscription, World, ok, passport};

const BOARD_ARCHIVE: &str = "example:board_archive";
const RECV: Duration = Duration::from_secs(10);

#[tokio::test]
async fn a_non_rls_projector_returns_the_same_view_on_fetch_and_subscription_under_an_applier() {
    let world = World::start("pod-rls-parity-cohort").await;
    let org_a = Uuid::now_v7();
    let org_b = Uuid::now_v7();
    let owner_id = Uuid::now_v7();
    let reader_id = Uuid::now_v7();
    let owner = passport(owner_id, org_a, &[BOARD_ARCHIVE], false);
    let reader = passport(reader_id, org_b, &[], false);

    let board = Uuid::now_v7();
    ok(&world
        .gql(
            &owner,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": board, "n": "Cross-org Private" }),
        )
        .await);
    ok(&world
        .gql(
            &owner,
            "mutation($b:UUID!,$u:UUID!){exampleSetBoardMembership(boardId:$b,userId:$u,member:true){success}}",
            serde_json::json!({ "b": board, "u": reader_id }),
        )
        .await);

    let fetched = ok(&world
        .gql(
            &reader,
            "query($id:UUID!){exampleBoard(id:$id){id name isPublic archived affordances}}",
            serde_json::json!({ "id": board }),
        )
        .await)["exampleBoard"]
        .clone();
    assert_eq!(
        fetched["id"],
        board.to_string(),
        "the cross-org member fetches the board through the non-RLS cohort path, even though an \
         RlsApplier is registered; the old rls_engaged() heuristic would have engaged RLS and \
         returned null here"
    );

    let query = "subscription{exampleBoardDeltas{\
        __typename \
        ... on BoardReset{views{... on BoardView{id name isPublic archived affordances}}}}}";
    let mut sub = Subscription::open(&world.subscription_url(), &reader, query).await;
    let reset = sub.next_payload(RECV).await;
    assert_eq!(reset["exampleBoardDeltas"]["__typename"], "BoardReset");
    let from_stream = reset["exampleBoardDeltas"]["views"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["id"] == board.to_string())
        .expect("the subscription's Reset carries the cross-org board")
        .clone();

    assert_eq!(
        fetched, from_stream,
        "a fetch and a subscription of the same key by the same principal are one code path: the \
         two views are byte-identical, because both derive the render regime from the projector"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn an_rls_projector_hides_a_foreign_org_row_on_both_the_fetch_and_the_subscription() {
    let world = World::start("pod-rls-parity-strict").await;
    let org_a = Uuid::now_v7();
    let org_b = Uuid::now_v7();
    let principal = passport(Uuid::now_v7(), org_a, &[], false);
    let intruder = passport(Uuid::now_v7(), org_b, &[], false);

    let mine = Uuid::now_v7();
    let foreign = Uuid::now_v7();
    let public = Uuid::now_v7();
    ok(&world
        .gql(
            &principal,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": mine, "n": "Mine A" }),
        )
        .await);
    ok(&world
        .gql(
            &intruder,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": foreign, "n": "Theirs B" }),
        )
        .await);
    ok(&world
        .gql(
            &principal,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:true){success}}",
            serde_json::json!({ "id": public, "n": "Public" }),
        )
        .await);

    let fetched: Vec<String> = ok(&world
        .gql(&principal, "query{exampleOrgBoards{id}}", serde_json::json!({}))
        .await)["exampleOrgBoards"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["id"].as_str().unwrap().to_string())
        .collect();
    assert!(
        fetched.contains(&mine.to_string()),
        "the fetch returns the org's own board"
    );
    assert!(
        fetched.contains(&public.to_string()),
        "the fetch returns the public board"
    );
    assert!(
        !fetched.contains(&foreign.to_string()),
        "the RLS query-time load runs under the applier's org context, so the foreign-org row is \
         absent from the fetch: {fetched:?}"
    );

    let query = "subscription{exampleOrgBoardDeltas{\
        __typename \
        ... on OrgBoardReset{views{... on BoardView{id}}}}}";
    let mut sub = Subscription::open(&world.subscription_url(), &principal, query).await;
    let reset = sub.next_payload(RECV).await;
    assert_eq!(reset["exampleOrgBoardDeltas"]["__typename"], "OrgBoardReset");
    let streamed: Vec<String> = reset["exampleOrgBoardDeltas"]["views"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["id"].as_str().unwrap().to_string())
        .collect();

    let mut fetched_sorted = fetched.clone();
    fetched_sorted.sort();
    let mut streamed_sorted = streamed.clone();
    streamed_sorted.sort();
    assert_eq!(
        fetched_sorted, streamed_sorted,
        "the subscription's Reset over the RLS projector holds the same keys as the fetch, since \
         both render under the projector-declared RLS regime"
    );
    assert!(
        !streamed.contains(&foreign.to_string()),
        "the foreign-org row is hidden on the subscription channel too: {streamed:?}"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn the_rls_projector_populates_its_window_under_the_org_context_not_on_a_bare_pool() {
    let world = World::start("pod-rls-populate-scoped").await;
    let org_a = Uuid::now_v7();
    let org_b = Uuid::now_v7();
    let principal = passport(Uuid::now_v7(), org_a, &[], false);
    let intruder = passport(Uuid::now_v7(), org_b, &[], false);

    let mine = Uuid::now_v7();
    let foreign = Uuid::now_v7();
    let public = Uuid::now_v7();
    ok(&world
        .gql(
            &principal,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": mine, "n": "Mine A" }),
        )
        .await);
    ok(&world
        .gql(
            &intruder,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": foreign, "n": "Theirs B" }),
        )
        .await);
    ok(&world
        .gql(
            &intruder,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:true){success}}",
            serde_json::json!({ "id": public, "n": "Public B" }),
        )
        .await);

    let pool = &world.db.app;

    let mut scoped_tx = pool.begin().await.expect("scoped tx on the app pool");
    sqlx::query("SELECT set_config('app.current_org_id', $1, true)")
        .bind(org_a.to_string())
        .execute(&mut *scoped_tx)
        .await
        .expect("set the org context transaction-locally");
    let scoped: Vec<Uuid> = sqlx::query("SELECT id FROM org_board")
        .fetch_all(&mut *scoped_tx)
        .await
        .expect("read the org_board projection under the org context")
        .iter()
        .map(|row| row.get::<Uuid, _>("id"))
        .collect();
    scoped_tx
        .rollback()
        .await
        .expect("rollback the scoped read");

    assert!(
        scoped.contains(&mine) && scoped.contains(&public) && !scoped.contains(&foreign),
        "OrgBoardsRls reads the org_board projection under the applier's org context, so the \
         window is narrowed to the org's own boards plus public ones at populate time; the \
         foreign-org row is never a window member, not merely filtered at render: {scoped:?}"
    );

    let mut unset_tx = pool.begin().await.expect("unset tx on the app pool");
    let unset: Vec<Uuid> = sqlx::query("SELECT id FROM org_board")
        .fetch_all(&mut *unset_tx)
        .await
        .expect("read the org_board projection with no org context set")
        .iter()
        .map(|row| row.get::<Uuid, _>("id"))
        .collect();
    unset_tx.rollback().await.expect("rollback the unset read");

    assert!(
        unset.is_empty(),
        "the RLS projection is deny-when-unset: with no org context in the transaction the \
         org_board projection returns no rows at all, so a read that forgets the context leaks \
         nothing instead of falling back to every row: {unset:?}"
    );

    let mut cohort_tx = pool.begin().await.expect("cohort tx on the app pool");
    let cohort: Vec<Uuid> = sqlx::query("SELECT id FROM board")
        .fetch_all(&mut *cohort_tx)
        .await
        .expect("read the base board table the cohort projector uses")
        .iter()
        .map(|row| row.get::<Uuid, _>("id"))
        .collect();
    cohort_tx
        .rollback()
        .await
        .expect("rollback the cohort read");

    assert!(
        cohort.contains(&mine) && cohort.contains(&foreign) && cohort.contains(&public),
        "the base board table carries no RLS: it is the non-RLS cohort projector BoardsView's \
         source, which must see every candidate so Board::memberships can grant a cross-org \
         Member visibility in the app layer that a fail-closed org policy would wrongly hide. The \
         two regimes therefore sit on two sources — a fail-closed org_board projection for RLS, \
         the plain board table for cohorts — and the regime is the projector's, never the \
         table's: {cohort:?}"
    );

    world.cleanup().await;
}
