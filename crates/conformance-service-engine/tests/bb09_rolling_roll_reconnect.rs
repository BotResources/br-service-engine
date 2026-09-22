mod blackbox_support;

use std::time::Duration;

use blackbox_support::{GraphqlWs, World, ok, passport};
use uuid::Uuid;

const BOARD_ARCHIVE: &str = "example:board_archive";
const RECV: Duration = Duration::from_secs(15);
const SUB: &str = "subscription{exampleBoardDeltas{__typename \
    ... on BoardReset{revision views{... on BoardView{id archived}}} \
    ... on BoardUpsert{revision view{... on BoardView{id archived}}} \
    ... on BoardRemove{revision}}}";

#[tokio::test]
async fn bb09_a_client_survives_its_pod_being_rolled_by_reconnecting_to_another() {
    let world = World::start("bb09-survivor").await;
    let doomed = world.spawn_pod("bb09-doomed").await;

    let org = Uuid::now_v7();
    let user = Uuid::now_v7();
    let pass = passport(user, org, &[BOARD_ARCHIVE], false);
    let board = Uuid::now_v7();

    ok(&world
        .gql_at(
            doomed.base_url(),
            &pass,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": board, "n": "Rolled board" }),
        )
        .await);

    let mut ws = GraphqlWs::connect(doomed.base_url(), &pass).await;
    ws.subscribe("1", SUB).await;
    let reset = ws
        .next_data(RECV)
        .await
        .expect("the doomed pod opens the session with a Reset");
    assert_eq!(reset["exampleBoardDeltas"]["__typename"], "BoardReset");

    ok(&world
        .gql_at(
            doomed.base_url(),
            &pass,
            "mutation($id:UUID!){exampleArchiveBoard(id:$id){success}}",
            serde_json::json!({ "id": board }),
        )
        .await);
    let upsert = loop {
        let delta = ws
            .next_data(RECV)
            .await
            .expect("the doomed pod delivers the archive delta while the client is attached");
        if delta["exampleBoardDeltas"]["__typename"] == "BoardUpsert" {
            break delta;
        }
    };
    assert_eq!(
        upsert["exampleBoardDeltas"]["view"]["archived"],
        serde_json::json!(true),
        "the client saw the archive land on the doomed pod: {upsert}"
    );

    doomed.shutdown().await;
    drop(ws);

    let mut reconnected = GraphqlWs::connect(world.base_url(), &pass).await;
    reconnected.subscribe("1", SUB).await;
    let fresh = reconnected
        .next_data(RECV)
        .await
        .expect("the surviving pod opens a fresh session for the reconnecting client");
    assert_eq!(fresh["exampleBoardDeltas"]["__typename"], "BoardReset");
    assert_eq!(
        fresh["exampleBoardDeltas"]["revision"],
        serde_json::json!(1),
        "the reconnect is a new session opening at revision 1: {fresh}"
    );
    let carries_committed = fresh["exampleBoardDeltas"]["views"]
        .as_array()
        .expect("the reconnect Reset carries its views")
        .iter()
        .any(|view| view["id"] == board.to_string() && view["archived"] == serde_json::json!(true));
    assert!(
        carries_committed,
        "the surviving pod's Reset shows the board archived on the rolled pod — committed state crosses pods: {fresh}"
    );

    world.cleanup().await;
}
