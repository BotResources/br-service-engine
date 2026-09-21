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
async fn bb08_a_mutation_on_one_pod_reaches_a_session_attached_to_another_pod() {
    let world = World::start("bb08-pod-a").await;
    let pod_b = world.spawn_pod("bb08-pod-b").await;

    let org = Uuid::now_v7();
    let user = Uuid::now_v7();
    let pass = passport(user, org, &[BOARD_ARCHIVE], false);
    let board = Uuid::now_v7();

    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": board, "n": "Cross-pod board" }),
        )
        .await);

    let mut ws = GraphqlWs::connect(pod_b.base_url(), &pass).await;
    ws.subscribe("1", SUB).await;
    let reset = ws
        .next_data(RECV)
        .await
        .expect("the attach on pod B delivers a Reset");
    assert_eq!(reset["exampleBoardDeltas"]["__typename"], "BoardReset");
    let carries_board = reset["exampleBoardDeltas"]["views"]
        .as_array()
        .expect("the Reset carries its views")
        .iter()
        .any(|view| view["id"] == board.to_string());
    assert!(
        carries_board,
        "pod B's Reset is rendered from committed state, so it shows the board created on pod A: {reset}"
    );

    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!){exampleArchiveBoard(id:$id){success}}",
            serde_json::json!({ "id": board }),
        )
        .await);

    let upsert = loop {
        let delta = ws
            .next_data(RECV)
            .await
            .expect("the archive committed on pod A reaches the session on pod B");
        if delta["exampleBoardDeltas"]["__typename"] == "BoardUpsert" {
            break delta;
        }
    };
    assert_eq!(
        upsert["exampleBoardDeltas"]["revision"],
        serde_json::json!(2),
        "the delta advances pod B's session by exactly one revision: {upsert}"
    );
    assert_eq!(
        upsert["exampleBoardDeltas"]["view"]["id"],
        board.to_string(),
        "the cross-pod delta names the board mutated on pod A: {upsert}"
    );
    assert_eq!(
        upsert["exampleBoardDeltas"]["view"]["archived"],
        serde_json::json!(true),
        "the committed archive from pod A — not a refetch — reached the session on pod B: {upsert}"
    );

    pod_b.shutdown().await;
    world.cleanup().await;
}
