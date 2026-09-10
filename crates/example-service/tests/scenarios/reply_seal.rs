use std::time::Duration;

use example_contract::ReplyFinished;
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
            "mutation($id:UUID!,$b:UUID!){startReply(id:$id,boardId:$b){success}}",
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
async fn a_finish_below_a_durable_chunk_is_answered_with_seal_failed_not_retried_forever() {
    let world = World::start("pod-reply-seal-failed").await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let board = Uuid::now_v7();
    let chunks = ["Hel", "lo ", "wor", "ld"];

    let reply = start_and_stream(&world, &pass, board, &chunks).await;

    example_twin::send_reply_finished(
        &world.nats,
        &ReplyFinished {
            reply_id: reply,
            board_id: board,
            last_seq: 1,
            hash: hash_of(&chunks[..2]),
        },
    )
    .await
    .unwrap();

    let failed = poll_until!(Duration::from_secs(10), {
        example_twin::seal_failed_from_stream(&world.nats)
            .await
            .expect("reading the seal-failed subject")
    });
    assert_eq!(
        failed.reply_id, reply,
        "the runner is told the seal failed instead of the finish retrying forever"
    );
    assert!(
        !failed.reason.is_empty(),
        "the SealFailed confirmation carries the reason the runner needs to resend"
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
        "a stream the finish declared too short is never silently saved; the reply stays open"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_permanently_truncated_stream_is_answered_with_seal_failed_after_the_retry_budget() {
    let world = World::start("pod-reply-truncated").await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let board = Uuid::now_v7();
    let chunks = ["Half ", "of a "];

    let reply = start_and_stream(&world, &pass, board, &chunks).await;

    example_twin::send_reply_finished(
        &world.nats,
        &ReplyFinished {
            reply_id: reply,
            board_id: board,
            last_seq: 3,
            hash: hash_of(&chunks),
        },
    )
    .await
    .unwrap();

    let failed = poll_until!(Duration::from_secs(20), {
        example_twin::seal_failed_from_stream(&world.nats)
            .await
            .expect("reading the seal-failed subject")
    });
    assert_eq!(
        failed.reply_id, reply,
        "a stream shorter than the declared last_seq is a truncation the fold cannot heal; after \
         the retry budget the runner is told to resend, never nak'd forever into a silent dead letter"
    );
    assert!(
        !failed.reason.is_empty(),
        "the SealFailed confirmation carries the reason the runner needs to resend"
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
        "a permanently truncated stream is never silently saved; the reply stays open for the resend"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_reply_whose_finish_declares_the_wrong_hash_is_never_sealed() {
    let world = World::start("pod-reply-integrity").await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let board = Uuid::now_v7();
    let chunks = ["Alpha", "Beta"];

    let tampered = start_and_stream(&world, &pass, board, &chunks).await;
    example_twin::send_reply_finished(
        &world.nats,
        &ReplyFinished {
            reply_id: tampered,
            board_id: board,
            last_seq: (chunks.len() - 1) as u64,
            hash: hash_of(&["Alpha", "Gamma"]),
        },
    )
    .await
    .unwrap();

    let control = start_and_stream(&world, &pass, board, &chunks).await;
    example_twin::send_reply_finished(
        &world.nats,
        &ReplyFinished {
            reply_id: control,
            board_id: board,
            last_seq: (chunks.len() - 1) as u64,
            hash: hash_of(&chunks),
        },
    )
    .await
    .unwrap();

    poll_until!(Duration::from_secs(5), {
        let view = world
            .gql(
                &pass,
                "query($id:UUID!){reply(id:$id){status}}",
                serde_json::json!({ "id": control }),
            )
            .await;
        (view["data"]["reply"]["status"] == "complete").then_some(())
    });

    world.service.settle().await;
    let tampered_view = world
        .gql(
            &pass,
            "query($id:UUID!){reply(id:$id){status text}}",
            serde_json::json!({ "id": tampered }),
        )
        .await;
    assert_eq!(
        ok(&tampered_view)["reply"]["status"],
        "streaming",
        "an altered stream is refused by the hash and never sealed silently"
    );
    assert_eq!(ok(&tampered_view)["reply"]["text"], "");

    world.cleanup().await;
}
