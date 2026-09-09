use std::time::Duration;

use uuid::Uuid;

use crate::harness::{Subscription, World, ok, passport};

const BOARD_ARCHIVE: &str = "example:board_archive";
const RECV: Duration = Duration::from_secs(10);

#[tokio::test]
async fn two_pods_serve_identical_committed_state() {
    let world = World::start("pod-a").await;
    let pod_b = world.boot_second_pod("pod-b").await;
    let org = Uuid::now_v7();
    let user = Uuid::now_v7();
    let pass = passport(user, org, &[BOARD_ARCHIVE], false);

    let b = Uuid::now_v7();
    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!,$n:String!){createBoard(id:$id,name:$n,isPublic:true){success}}",
            serde_json::json!({ "id": b, "n": "Shared" }),
        )
        .await);

    let on_b = world
        .gql_at(
            &pod_b,
            &pass,
            "query($id:UUID!){board(id:$id){name affordances}}",
            serde_json::json!({ "id": b }),
        )
        .await;
    assert_eq!(ok(&on_b)["board"]["name"], "Shared");
    assert_eq!(
        ok(&on_b)["board"]["affordances"]["archive"]["allowed"],
        true
    );

    pod_b.shutdown().await;
    world.cleanup().await;
}

#[tokio::test]
async fn a_subscriber_on_pod_b_receives_the_upsert_caused_by_a_write_on_pod_a() {
    let world = World::start("pod-a-write").await;
    let pod_b = world.boot_second_pod("pod-b-subscribe").await;
    let org = Uuid::now_v7();
    let user = Uuid::now_v7();
    let pass = passport(user, org, &[BOARD_ARCHIVE], false);

    let board = Uuid::now_v7();
    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!,$n:String!){createBoard(id:$id,name:$n,isPublic:true){success}}",
            serde_json::json!({ "id": board, "n": "Cross-pod" }),
        )
        .await);

    let query = "subscription{boardDeltas{\
        __typename \
        ... on BoardReset{revision views{... on BoardView{id name archived}}} \
        ... on BoardUpsert{revision view{... on BoardView{id name archived}}} \
        ... on BoardRemove{revision projector}}}";
    let mut sub = Subscription::open(&pod_b.ws("/graphql/ws"), &pass, query).await;

    let reset = sub.next_payload(RECV).await;
    assert_eq!(
        reset["boardDeltas"]["__typename"], "BoardReset",
        "the subscriber on pod B receives a Reset on attach"
    );

    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!){archiveBoard(id:$id){success}}",
            serde_json::json!({ "id": board }),
        )
        .await);

    let flip = loop {
        let delta = sub.next_payload(RECV).await;
        if delta["boardDeltas"]["__typename"] == "BoardUpsert"
            && delta["boardDeltas"]["view"]["id"] == board.to_string()
        {
            break delta;
        }
    };
    assert_eq!(
        flip["boardDeltas"]["view"]["archived"], true,
        "a write on pod A reaches the subscriber on pod B as an Upsert through the impact bus"
    );

    pod_b.shutdown().await;
    world.cleanup().await;
}
