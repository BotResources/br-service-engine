use std::time::Duration;

use uuid::Uuid;

use crate::harness::{Subscription, World, ok, passport};

const BOARD_ARCHIVE: &str = "example:board_archive";
const RECV: Duration = Duration::from_secs(10);
const CLOSE_WITHIN: Duration = Duration::from_secs(8);
const SESSION_TTL: Duration = Duration::from_millis(500);
const SESSION_MAX_AGE: Duration = Duration::from_millis(2000);
const CLOSE_CODE_GOING_AWAY: u16 = 1001;

const SUB: &str = "subscription{boardDeltas{\
    __typename \
    ... on BoardReset{revision views{... on BoardView{id archived affordances}}} \
    ... on BoardUpsert{revision view{... on BoardView{id archived affordances}}} \
    ... on BoardRemove{revision projector}}}";

#[tokio::test]
async fn a_socket_older_than_session_max_age_is_closed_and_a_fresh_socket_carries_the_new_passport()
{
    let world = World::start_bounded("pod-session-lifetime", SESSION_TTL, SESSION_MAX_AGE).await;
    let org = Uuid::now_v7();
    let user = Uuid::now_v7();
    let board = Uuid::now_v7();

    let with_archive = passport(user, org, &[BOARD_ARCHIVE], false);
    ok(&world
        .gql(
            &with_archive,
            "mutation($id:UUID!,$n:String!){createBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": board, "n": "Session board" }),
        )
        .await);

    let mut aged = Subscription::open(&world.subscription_url(), &with_archive, SUB).await;
    let reset = aged.next_payload(RECV).await;
    assert_eq!(
        reset["boardDeltas"]["__typename"], "BoardReset",
        "the socket attaches with a Reset"
    );
    let allowed_now = reset["boardDeltas"]["views"]
        .as_array()
        .expect("the Reset carries its views")
        .iter()
        .find(|view| view["id"] == board.to_string())
        .map(|view| view["affordances"]["archive"]["allowed"].clone())
        .expect("the board is in the window");
    assert_eq!(
        allowed_now,
        serde_json::json!(true),
        "the handshake passport carries the archive scope, so the affordance is allowed: {reset}"
    );

    let (code, reason) = aged.expect_close(CLOSE_WITHIN).await;
    assert_eq!(
        code, CLOSE_CODE_GOING_AWAY,
        "past session_max_age the kit closes the socket itself with the going-away code, got \
         {code} ({reason})"
    );
    assert!(
        reason.contains("session_max_age"),
        "the close reason names the bound so the client reconnects with a fresh passport: {reason}"
    );
    assert!(
        aged.resubscribe_refused("2", SUB).await,
        "a re-subscribe on the closed socket is impossible, so the handshake passport cannot serve \
         the socket past the bound"
    );

    let revoked = passport(user, org, &[], false);
    let mut fresh = Subscription::open(&world.subscription_url(), &revoked, SUB).await;
    let after = fresh.next_payload(RECV).await;
    assert_eq!(
        after["boardDeltas"]["__typename"], "BoardReset",
        "the fresh socket attaches with a Reset"
    );
    let allowed_after = after["boardDeltas"]["views"]
        .as_array()
        .expect("the Reset carries its views")
        .iter()
        .find(|view| view["id"] == board.to_string())
        .map(|view| view["affordances"]["archive"]["allowed"].clone())
        .expect("the board is still visible");
    assert_eq!(
        allowed_after,
        serde_json::json!(false),
        "the fresh socket carries the revoked passport the gateway re-injects, so the archive \
         affordance flips to blocked within session_max_age of the change: {after}"
    );

    world.cleanup().await;
}
