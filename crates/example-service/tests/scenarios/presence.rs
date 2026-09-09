use std::time::Duration;

use service_engine::nats::KvKey;
use uuid::Uuid;

use crate::harness::{World, error_code, ok, passport};
use crate::poll_until;

async fn create_board(world: &World, pass: &str, board: Uuid) {
    ok(&world
        .gql(
            pass,
            "mutation($id:UUID!,$n:String!){createBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": board, "n": "drafts" }),
        )
        .await);
}

#[tokio::test]
async fn presence_lane_writes_a_ttl_value() {
    let world = World::start("pod-presence").await;
    let org = Uuid::now_v7();
    let user = Uuid::now_v7();
    let pass = passport(user, org, &[], false);
    let board = Uuid::now_v7();

    create_board(&world, &pass, board).await;

    ok(&world
        .gql(
            &pass,
            "mutation($b:UUID!,$l:String!){setTyping(board:$b,label:$l){success}}",
            serde_json::json!({ "b": board, "l": "drafting" }),
        )
        .await);

    let bucket = world
        .nats
        .bind_ephemeral::<serde_json::Value>(&format!("EPHEMERAL_{}", example_contract::SERVICE))
        .await
        .unwrap();
    let key = KvKey::new(format!("typing/{}/{}", board.simple(), user.simple())).unwrap();
    let value = poll_until!(Duration::from_secs(5), {
        bucket
            .get(&key)
            .await
            .unwrap()
            .and_then(|v| v["label"].as_str().map(str::to_string))
    });
    assert_eq!(value, "drafting");

    world.cleanup().await;
}

#[tokio::test]
async fn set_typing_by_a_non_member_is_refused() {
    let world = World::start("pod-presence-gate").await;
    let org = Uuid::now_v7();
    let owner = Uuid::now_v7();
    let outsider = Uuid::now_v7();
    let board = Uuid::now_v7();

    create_board(&world, &passport(owner, org, &[], false), board).await;

    let refused = world
        .gql(
            &passport(outsider, org, &[], false),
            "mutation($b:UUID!,$l:String!){setTyping(board:$b,label:$l){success}}",
            serde_json::json!({ "b": board, "l": "sneaking in" }),
        )
        .await;
    assert_eq!(
        error_code(&refused),
        "not_a_board_member",
        "a person who is not a member of the board cannot signal typing on it"
    );

    let bucket = world
        .nats
        .bind_ephemeral::<serde_json::Value>(&format!("EPHEMERAL_{}", example_contract::SERVICE))
        .await
        .unwrap();
    let key = KvKey::new(format!("typing/{}/{}", board.simple(), outsider.simple())).unwrap();
    assert!(
        bucket.get(&key).await.unwrap().is_none(),
        "the refused presence write never reached the bucket"
    );

    world.cleanup().await;
}
