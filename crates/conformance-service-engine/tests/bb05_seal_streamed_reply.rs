mod blackbox_support;

use std::time::{Duration, Instant};

use blackbox_support::{World, ok, passport};
use example_contract::ReplyFinished;
use service_engine::SealHash;
use uuid::Uuid;

fn hash_of(chunks: &[&str]) -> String {
    SealHash::of_chunks(chunks.iter().map(|c| c.to_string()))
        .expect("hash the chunks")
        .to_hex()
}

#[tokio::test]
async fn bb05_a_streamed_reply_seals_against_its_hash_in_the_running_binary() {
    let world = World::start("bb05-pod").await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let board = Uuid::now_v7();
    let reply = Uuid::now_v7();
    let chunks = ["Hel", "lo ", "world"];

    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!,$b:UUID!){exampleStartReply(id:$id,boardId:$b){success}}",
            serde_json::json!({ "id": reply, "b": board }),
        )
        .await);

    example_twin::stream_reply(&world.nats, reply, &chunks)
        .await
        .expect("a separate producer streams the reply chunks over NATS, not through Postgres");

    example_twin::send_reply_finished(
        &world.nats,
        &ReplyFinished {
            reply_id: reply,
            board_id: board,
            last_seq: (chunks.len() - 1) as u64,
            hash: hash_of(&chunks),
        },
    )
    .await
    .expect("publish the finish command to the running binary over NATS");

    let deadline = Instant::now() + Duration::from_secs(15);
    let text = loop {
        let view = world
            .gql(
                &pass,
                "query($id:UUID!){exampleReply(id:$id){text status}}",
                serde_json::json!({ "id": reply }),
            )
            .await;
        if ok(&view)["exampleReply"]["status"] == "complete" {
            break ok(&view)["exampleReply"]["text"].as_str().unwrap().to_string();
        }
        assert!(
            Instant::now() < deadline,
            "the running binary's reply-finished reaction never sealed the stream to a complete record: {view}"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    };
    assert_eq!(
        text, "Hello world",
        "seal replays the chunks in the running binary, verifies them against the hash, \
         and commits the concatenated text as the reply record — read back over GraphQL"
    );

    world.cleanup().await;
}
