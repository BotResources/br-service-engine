use std::time::Duration;

use example_contract::{ReplyCancelled, ReplyFinished};
use service_engine::SealHash;
use service_engine::accumulator::ChunkSeq;
use service_engine::nats::INTEGRATION_CMD;
use uuid::Uuid;

use crate::harness::{World, ok, passport};
use crate::poll_until;

const OUT_OF_RANGE_SEQ: u64 = ChunkSeq::MAX + 1;

async fn start_reply(world: &World, pass: &str, board: Uuid) -> Uuid {
    let reply = Uuid::now_v7();
    ok(&world
        .gql(
            pass,
            "mutation($id:UUID!,$b:UUID!){exampleStartReply(id:$id,boardId:$b){success}}",
            serde_json::json!({ "id": reply, "b": board }),
        )
        .await);
    reply
}

async fn reply_status(world: &World, pass: &str, reply: Uuid) -> serde_json::Value {
    let view = world
        .gql(
            pass,
            "query($id:UUID!){exampleReply(id:$id){status}}",
            serde_json::json!({ "id": reply }),
        )
        .await;
    ok(&view)["exampleReply"]["status"].clone()
}

async fn dead_letters_of(world: &World, message_id: Uuid) -> Vec<(String, i32, String)> {
    sqlx::query_as(
        "SELECT reaction, delivered, error FROM service_engine.dead_letter WHERE message_id = $1",
    )
    .bind(message_id)
    .fetch_all(&world.db.app)
    .await
    .unwrap()
}

async fn settled_deliveries(world: &World, durable: &str) -> Option<u64> {
    let info = world
        .nats
        .context()
        .get_stream(INTEGRATION_CMD)
        .await
        .expect("binding the command stream")
        .consumer_info(durable)
        .await
        .expect("reading the reaction's durable");
    (info.num_pending == 0 && info.num_ack_pending == 0).then_some(info.delivered.consumer_sequence)
}

async fn assert_dead_lettered_on_its_only_delivery(world: &World, message_id: Uuid) {
    let rows = poll_until!(Duration::from_secs(10), {
        let rows = dead_letters_of(world, message_id).await;
        (!rows.is_empty()).then_some(rows)
    });
    assert_eq!(rows.len(), 1, "one dead-letter row for the message");
    let (reaction, delivered, error) = &rows[0];
    assert_eq!(
        *delivered, 1,
        "a sequence no bigint holds can never succeed, so the frame is dead-lettered on its first \
         delivery instead of spending the retry budget"
    );
    assert!(
        error.contains(&format!("chunk sequence {OUT_OF_RANGE_SEQ} is above")),
        "the dead-letter reason names the out-of-range sequence: {error}"
    );

    let deliveries = poll_until!(Duration::from_secs(10), {
        settled_deliveries(world, reaction).await
    });
    assert_eq!(
        deliveries, 1,
        "the durable settled the frame after that one delivery: no redelivery loop"
    );
}

#[tokio::test]
async fn a_seal_command_whose_last_seq_no_bigint_holds_is_dead_lettered_once_never_retried() {
    let world = World::start("pod-reply-malformed").await;
    let pass = passport(Uuid::now_v7(), Uuid::now_v7(), &[], false);
    let board = Uuid::now_v7();
    let hash = SealHash::of_chunks(["Hel".to_string()]).unwrap().to_hex();
    let finished = start_reply(&world, &pass, board).await;
    let cancelled = start_reply(&world, &pass, board).await;

    example_twin::send_reply_finished(
        &world.nats,
        &ReplyFinished {
            reply_id: finished,
            board_id: board,
            last_seq: OUT_OF_RANGE_SEQ,
            hash: hash.clone(),
        },
    )
    .await
    .unwrap();
    example_twin::send_reply_cancelled(
        &world.nats,
        &ReplyCancelled {
            reply_id: cancelled,
            board_id: board,
            last_seq: OUT_OF_RANGE_SEQ,
            hash,
        },
    )
    .await
    .unwrap();

    for reply in [finished, cancelled] {
        assert_dead_lettered_on_its_only_delivery(&world, reply).await;
        assert_eq!(
            reply_status(&world, &pass, reply).await,
            "streaming",
            "a seal command the service can never apply leaves the reply open"
        );
    }

    world.cleanup().await;
}
