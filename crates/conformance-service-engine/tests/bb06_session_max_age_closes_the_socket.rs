mod blackbox_support;

use std::time::Duration;

use blackbox_support::{GraphqlWs, SessionBounds, World, ok, passport};
use uuid::Uuid;

const BOARD_ARCHIVE: &str = "example:board_archive";
const RECV: Duration = Duration::from_secs(15);
const CLOSE_WITHIN: Duration = Duration::from_secs(10);
const SESSION_TTL: Duration = Duration::from_millis(500);
const SESSION_MAX_AGE: Duration = Duration::from_millis(2000);
const CLOSE_CODE_GOING_AWAY: u16 = 1001;
const SUB: &str = "subscription{boardDeltas{__typename \
    ... on BoardReset{revision views{... on BoardView{id archived}}} \
    ... on BoardUpsert{revision view{... on BoardView{id archived}}} \
    ... on BoardRemove{revision}}}";

#[tokio::test]
async fn bb06_a_socket_older_than_session_max_age_is_closed_and_cannot_resubscribe() {
    let world = World::start_with_bounds(
        "bb06-pod",
        Some(SessionBounds {
            ttl: SESSION_TTL,
            max_age: SESSION_MAX_AGE,
        }),
    )
    .await;

    let org = Uuid::now_v7();
    let user = Uuid::now_v7();
    let pass = passport(user, org, &[BOARD_ARCHIVE], false);
    let board = Uuid::now_v7();

    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!,$n:String!){createBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": board, "n": "Aged board" }),
        )
        .await);

    let mut ws = GraphqlWs::connect(world.base_url(), &pass).await;
    ws.subscribe("1", SUB).await;
    let reset = ws
        .next_data(RECV)
        .await
        .expect("the fresh socket attaches and delivers a Reset");
    assert_eq!(
        reset["boardDeltas"]["__typename"], "BoardReset",
        "a fresh socket opens with a Reset: {reset}"
    );

    let (code, reason) = ws.expect_close(CLOSE_WITHIN).await;
    assert_eq!(
        code, CLOSE_CODE_GOING_AWAY,
        "past session_max_age the kit closes the connection itself with the going-away code so the \
         client reconnects, got {code} ({reason})"
    );
    assert!(
        reason.contains("session_max_age"),
        "the close reason names the bound so the client knows to reconnect with a fresh passport: \
         {reason}"
    );

    assert!(
        ws.resubscribe_refused("2", SUB).await,
        "the closed socket refuses any further subscribe, so a revoked scope cannot keep an \
         affordance allowed by re-subscribing on the same socket past the bound"
    );

    let mut fresh = GraphqlWs::connect(world.base_url(), &pass).await;
    fresh.subscribe("1", SUB).await;
    let fresh_reset = fresh
        .next_data(RECV)
        .await
        .expect("a fresh socket still attaches: the bound is on the connection, not the service");
    assert_eq!(
        fresh_reset["boardDeltas"]["__typename"], "BoardReset",
        "reconnecting on a new socket opens a new session from committed state: {fresh_reset}"
    );

    world.cleanup().await;
}
