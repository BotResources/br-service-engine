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
    let finished = ReplyFinished {
        reply_id: reply,
        board_id: board,
        last_seq: (chunks.len() - 1) as u64,
        hash: hash_of(&chunks),
    };

    let text = poll_until!(Duration::from_secs(10), {
        example_twin::resend_reply_finished(&world.nats, &finished)
            .await
            .unwrap();
        let view = world
            .gql(
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
        "the pod that owns the finish folds the NATS-streamed chunks and delivers the sealed reply"
    );

    let text_b = poll_until!(Duration::from_secs(10), {
        let view = world
            .gql_at(
                &second,
                &pass,
                "query($id:UUID!){reply(id:$id){text status}}",
                serde_json::json!({ "id": reply }),
            )
            .await;
        (view["data"]["reply"]["status"] == "complete")
            .then(|| view["data"]["reply"]["text"].as_str().unwrap().to_string())
    });
    assert_eq!(
        text_b, "Hello NATS",
        "the seal's impact reaches the second pod, which converges on the saved text"
    );

    second.shutdown().await;
    world.cleanup().await;
}

#[tokio::test]
async fn a_permanently_truncated_stream_is_answered_with_seal_failed_not_retried_forever() {
    let world = World::start("pod-seal-failed").await;
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
        .expect("the runner streams three chunks over NATS");

    example_twin::send_reply_finished(
        &world.nats,
        &ReplyFinished {
            reply_id: reply,
            board_id: board,
            last_seq: 5,
            hash: hash_of(&chunks),
        },
    )
    .await
    .expect("the runner declares a last sequence the stream never reaches");

    let failed = poll_until!(Duration::from_secs(10), {
        example_twin::seal_failed_from_stream(&world.nats)
            .await
            .expect("reading the seal-failed subject")
    });
    assert_eq!(
        failed.reply_id, reply,
        "the runner is told which key failed to seal instead of the finish retrying forever"
    );
    assert!(
        !failed.reason.is_empty(),
        "the SealFailed confirmation carries the reason the runner needs to republish"
    );

    let view = world
        .gql(
            &pass,
            "query($id:UUID!){reply(id:$id){status text}}",
            serde_json::json!({ "id": reply }),
        )
        .await;
    assert_eq!(
        view["data"]["reply"]["status"], "streaming",
        "a stream that failed to seal is never silently saved; the reply stays open"
    );

    world.cleanup().await;
}
