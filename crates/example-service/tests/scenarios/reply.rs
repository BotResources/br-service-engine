use std::time::Duration;

use example_contract::{ReplyCancelled, ReplyFinished};
use example_service::slices::reply::ReplyText;
use service_engine::SealHash;
use service_engine::accumulator::ChunkSeq;
use uuid::Uuid;

use crate::harness::{World, ok, passport};
use crate::poll_until;

async fn start_and_stream(world: &World, pass: &str, board: Uuid, chunks: &[&str]) -> Uuid {
    let reply = Uuid::now_v7();
    ok(&world
        .gql(
            pass,
            "mutation($id:UUID!,$b:UUID!){exampleStartReply(id:$id,boardId:$b){success}}",
            serde_json::json!({ "id": reply, "b": board }),
        )
        .await);
    for (seq, chunk) in chunks.iter().enumerate() {
        world
            .service
            .chunks
            .push_chunk::<ReplyText>(
                &reply,
                ChunkSeq::new(seq as u64).unwrap(),
                chunk.to_string(),
            )
            .unwrap()
            .await
            .unwrap();
    }
    reply
}

fn hash_of(chunks: &[&str]) -> String {
    SealHash::of_chunks(chunks.iter().map(|c| c.to_string()))
        .unwrap()
        .to_hex()
}

#[tokio::test]
async fn accumulated_lane_seals_the_streamed_reply_with_a_verified_hash() {
    let world = World::start("pod-reply").await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let board = Uuid::now_v7();
    let chunks = ["Hel", "lo ", "world"];

    let reply = start_and_stream(&world, &pass, board, &chunks).await;

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

    let text = poll_until!(Duration::from_secs(5), {
        let view = world
            .gql(
                &pass,
                "query($id:UUID!){exampleReply(id:$id){text status}}",
                serde_json::json!({ "id": reply }),
            )
            .await;
        (view["data"]["exampleReply"]["status"] == "complete")
            .then(|| view["data"]["exampleReply"]["text"].as_str().unwrap().to_string())
    });
    assert_eq!(text, "Hello world");

    world.cleanup().await;
}

#[tokio::test]
async fn cancelling_a_reply_seals_the_partial_stream_as_cancelled() {
    let world = World::start("pod-reply-cancel").await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let board = Uuid::now_v7();
    let chunks = ["Onc", "e up", "on a "];

    let reply = start_and_stream(&world, &pass, board, &chunks).await;

    world.service.settle().await;
    let streaming = world
        .gql(
            &pass,
            "query($id:UUID!){exampleReply(id:$id){status affordances}}",
            serde_json::json!({ "id": reply }),
        )
        .await;
    assert_eq!(ok(&streaming)["exampleReply"]["status"], "streaming");
    assert_eq!(
        ok(&streaming)["exampleReply"]["affordances"]["cancel"]["allowed"],
        true,
        "the cancel affordance and the cancel gate are one function: allowed while streaming"
    );

    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!){exampleCancelReply(id:$id){success}}",
            serde_json::json!({ "id": reply }),
        )
        .await);

    example_twin::send_reply_cancelled(
        &world.nats,
        &ReplyCancelled {
            reply_id: reply,
            board_id: board,
            last_seq: (chunks.len() - 1) as u64,
            hash: hash_of(&chunks),
        },
    )
    .await
    .unwrap();

    let text = poll_until!(Duration::from_secs(5), {
        let view = world
            .gql(
                &pass,
                "query($id:UUID!){exampleReply(id:$id){text status}}",
                serde_json::json!({ "id": reply }),
            )
            .await;
        (view["data"]["exampleReply"]["status"] == "cancelled")
            .then(|| view["data"]["exampleReply"]["text"].as_str().unwrap().to_string())
    });
    assert_eq!(
        text, "Once upon a ",
        "seal_partial saves what the stream held up to the cancel point, verified by the hash"
    );

    let cancelled = world
        .gql(
            &pass,
            "query($id:UUID!){exampleReply(id:$id){affordances}}",
            serde_json::json!({ "id": reply }),
        )
        .await;
    assert_eq!(
        ok(&cancelled)["exampleReply"]["affordances"]["cancel"]["allowed"],
        false,
        "a cancelled reply can no longer be cancelled — the gate blocks it and the affordance says so"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_lost_producer_is_caught_by_the_scheduled_cancel_deadline() {
    let world = World::start("pod-reply-deadline").await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let board = Uuid::now_v7();
    let chunks = ["Half ", "a tho", "ught"];

    let reply = start_and_stream(&world, &pass, board, &chunks).await;

    world.service.settle().await;
    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!){exampleCancelReply(id:$id){success}}",
            serde_json::json!({ "id": reply }),
        )
        .await);

    let cancelling = world
        .gql(
            &pass,
            "query($id:UUID!){exampleReply(id:$id){status}}",
            serde_json::json!({ "id": reply }),
        )
        .await;
    assert_eq!(
        ok(&cancelling)["exampleReply"]["status"],
        "cancelling",
        "the decision is written on the direct lane while the producer is still expected to answer"
    );

    sqlx::query("UPDATE service_engine.scheduled_message SET at = now() - interval '1 minute'")
        .execute(&world.db.app)
        .await
        .expect("fast-forward the scheduled cancel deadline so the beat fires it now");

    let text = poll_until!(Duration::from_secs(5), {
        let view = world
            .gql(
                &pass,
                "query($id:UUID!){exampleReply(id:$id){text status}}",
                serde_json::json!({ "id": reply }),
            )
            .await;
        (view["data"]["exampleReply"]["status"] == "cancelled")
            .then(|| view["data"]["exampleReply"]["text"].as_str().unwrap().to_string())
    });
    assert_eq!(
        text, "Half a thought",
        "with no producer answer, the scheduled deadline seals whatever the stream held through \
         seal_current and records it as cancelled"
    );

    world.cleanup().await;
}
