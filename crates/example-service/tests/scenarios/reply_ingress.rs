use std::time::Duration;

use example_contract::ReplyFinished;
use service_engine::SealHash;
use uuid::Uuid;

use crate::harness::{World, ok, passport};
use crate::poll_until;

fn hash_of(chunks: &[&str]) -> String {
    SealHash::of_chunks(chunks.iter().map(|c| c.to_string()))
        .unwrap()
        .to_hex()
}

#[tokio::test]
async fn chunks_streamed_over_nats_by_a_separate_process_reach_viewers_on_two_pods() {
    let world = World::start("pod-reply-ingress-a").await;
    let second = world.boot_second_pod("pod-reply-ingress-b").await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let board = Uuid::now_v7();
    let reply = Uuid::now_v7();
    let chunks = ["Hel", "lo ", "NATS"];

    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!,$b:UUID!){startReply(id:$id,boardId:$b){success}}",
            serde_json::json!({ "id": reply, "b": board }),
        )
        .await);

    example_twin::stream_reply(&world.nats, reply, &chunks)
        .await
        .expect("a separate producer streams the reply chunks over NATS, never through Postgres");
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
    .unwrap();

    for service in [&world.service, &second] {
        let text = poll_until!(Duration::from_secs(10), {
            let view = world
                .gql_at(
                    service,
                    &pass,
                    "query($id:UUID!){reply(id:$id){text status}}",
                    serde_json::json!({ "id": reply }),
                )
                .await;
            (view["data"]["reply"]["status"] == "complete")
                .then(|| view["data"]["reply"]["text"].as_str().unwrap().to_string())
        });
        assert_eq!(
            text, "Hello NATS",
            "every pod's ingress folds the NATS-streamed chunks and delivers the sealed reply"
        );
    }

    second.shutdown().await;
    world.cleanup().await;
}
