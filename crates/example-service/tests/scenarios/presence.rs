use std::time::Duration;

use service_engine::nats::KvKey;
use uuid::Uuid;

use crate::harness::{World, ok, passport};
use crate::poll_until;

#[tokio::test]
async fn presence_lane_writes_a_ttl_value() {
    let world = World::start("pod-presence").await;
    let org = Uuid::now_v7();
    let user = Uuid::now_v7();
    let pass = passport(user, org, &[], false);
    let board = Uuid::now_v7();

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
