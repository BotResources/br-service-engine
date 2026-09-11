mod blackbox_support;

use std::time::Duration;

use blackbox_support::{GraphqlWs, World, ok, passport};
use uuid::Uuid;

const BOARD_ARCHIVE: &str = "example:board_archive";
const RECV: Duration = Duration::from_secs(15);
const SUB: &str = "subscription{boardDeltas{__typename \
    ... on BoardReset{revision views{... on BoardView{id archived}}} \
    ... on BoardUpsert{revision view{... on BoardView{id archived}}} \
    ... on BoardRemove{revision}}}";

#[tokio::test]
async fn bb03_a_subscriber_resets_then_upserts_and_a_reconnect_resets_from_committed_state() {
    let world = World::start("bb03-pod").await;
    let org = Uuid::now_v7();
    let user = Uuid::now_v7();
    let pass = passport(user, org, &[BOARD_ARCHIVE], false);
    let board = Uuid::now_v7();

    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!,$n:String!){createBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": board, "n": "Sub board" }),
        )
        .await);

    let mut ws = GraphqlWs::connect(world.base_url(), &pass).await;
    ws.subscribe("1", SUB).await;
    let reset = ws
        .next_data(RECV)
        .await
        .expect("the attach delivers a Reset");
    assert_eq!(reset["boardDeltas"]["__typename"], "BoardReset");
    assert_eq!(
        reset["boardDeltas"]["revision"],
        serde_json::json!(1),
        "a session's first revision is 1: {reset}"
    );

    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!){archiveBoard(id:$id){success}}",
            serde_json::json!({ "id": board }),
        )
        .await);

    let upsert = loop {
        let delta = ws
            .next_data(RECV)
            .await
            .expect("committing the archive reaches the session");
        if delta["boardDeltas"]["__typename"] == "BoardUpsert" {
            break delta;
        }
    };
    assert_eq!(
        upsert["boardDeltas"]["revision"],
        serde_json::json!(2),
        "the next delta advances the revision by exactly one: {upsert}"
    );
    assert_eq!(
        upsert["boardDeltas"]["view"]["archived"],
        serde_json::json!(true),
        "the committed state, not the mutation response, reaches the session"
    );

    let mut reconnected = GraphqlWs::connect(world.base_url(), &pass).await;
    reconnected.subscribe("1", SUB).await;
    let fresh = reconnected
        .next_data(RECV)
        .await
        .expect("a reconnecting subscriber gets a fresh Reset");
    assert_eq!(fresh["boardDeltas"]["__typename"], "BoardReset");
    assert_eq!(
        fresh["boardDeltas"]["revision"],
        serde_json::json!(1),
        "the reconnect opens a new session at revision 1: {fresh}"
    );
    let carries_committed = fresh["boardDeltas"]["views"]
        .as_array()
        .expect("the Reset carries its views")
        .iter()
        .any(|view| view["id"] == board.to_string() && view["archived"] == serde_json::json!(true));
    assert!(
        carries_committed,
        "the reconnect Reset is rendered from committed state, so it shows the archived board: {fresh}"
    );

    world.cleanup().await;
}
