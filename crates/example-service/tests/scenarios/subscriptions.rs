use std::time::Duration;

use uuid::Uuid;

use crate::harness::{Subscription, World, ok, passport};

const BOARD_ARCHIVE: &str = "example:board_archive";
const RECV: Duration = Duration::from_secs(10);

#[tokio::test]
async fn a_subscriber_receives_reset_then_an_upsert_with_its_cause_over_the_wire() {
    let world = World::start("pod-sub-board").await;
    let org = Uuid::now_v7();
    let user = Uuid::now_v7();
    let pass = passport(user, org, &[BOARD_ARCHIVE], false);

    let board = Uuid::now_v7();
    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!,$n:String!){createBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": board, "n": "WS Board" }),
        )
        .await);

    let query = "subscription{boardDeltas{\
        __typename \
        ... on BoardReset{revision views{... on BoardView{id name archived}}} \
        ... on BoardUpsert{revision cause view{... on BoardView{id name archived}}} \
        ... on BoardRemove{revision projector}}}";
    let mut sub = Subscription::open(&world.subscription_url(), &pass, query).await;

    let reset = sub.next_payload(RECV).await;
    assert_eq!(
        reset["boardDeltas"]["__typename"], "BoardReset",
        "the first delta on attach is a Reset"
    );
    let seen = reset["boardDeltas"]["views"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v["name"] == "WS Board" && v["archived"] == false);
    assert!(seen, "the Reset snapshot carries the board's current state");

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
            && delta["boardDeltas"]["view"]["archived"] == true
        {
            break delta;
        }
    };
    assert_eq!(
        flip["boardDeltas"]["view"]["name"], "WS Board",
        "the Upsert carries the changed view over the wire"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn a_reply_upsert_carries_the_seal_cause_over_the_wire() {
    use example_contract::ReplyFinished;
    use example_service::slices::reply::ReplyText;
    use service_engine::SealHash;
    use service_engine::accumulator::ChunkSeq;

    let world = World::start("pod-sub-reply").await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let board = Uuid::now_v7();
    let reply = Uuid::now_v7();

    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!,$b:UUID!){startReply(id:$id,boardId:$b){success}}",
            serde_json::json!({ "id": reply, "b": board }),
        )
        .await);

    let query = "subscription{replyDeltas{\
        __typename \
        ... on ReplyReset{revision views{... on ReplyView{id status}}} \
        ... on ReplyUpsert{revision cause view{... on ReplyView{id status text}}} \
        ... on ReplyRemove{revision projector}}}";
    let mut sub = Subscription::open(&world.subscription_url(), &pass, query).await;
    let reset = sub.next_payload(RECV).await;
    assert_eq!(reset["replyDeltas"]["__typename"], "ReplyReset");

    let chunks = ["Hel", "lo ", "world"];
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
    let hash = SealHash::of_chunks(chunks.iter().map(|c| c.to_string()))
        .unwrap()
        .to_hex();
    example_twin::send_reply_finished(
        &world.nats,
        &ReplyFinished {
            reply_id: reply,
            board_id: board,
            last_seq: (chunks.len() - 1) as u64,
            hash,
        },
    )
    .await
    .unwrap();

    let sealed = loop {
        let delta = sub.next_payload(RECV).await;
        if delta["replyDeltas"]["__typename"] == "ReplyUpsert"
            && delta["replyDeltas"]["view"]["status"] == "complete"
        {
            break delta;
        }
    };
    assert_eq!(sealed["replyDeltas"]["view"]["text"], "Hello world");
    assert_eq!(
        sealed["replyDeltas"]["cause"]["kind"], "Completed",
        "the seal's domain cause rides along with the delta over the wire"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn the_typed_presence_union_delivers_typing_over_the_wire() {
    let world = World::start("pod-sub-typing").await;
    let org = Uuid::now_v7();
    let user = Uuid::now_v7();
    let pass = passport(user, org, &[], false);
    let board = Uuid::now_v7();

    let query = format!(
        "subscription{{typingDeltas(board:\"{board}\"){{\
            __typename \
            ... on TypingReset{{revision}} \
            ... on TypingUpsert{{view{{... on TypingView{{label}}}}}}}}}}"
    );
    let mut sub = Subscription::open(&world.subscription_url(), &pass, &query).await;
    let reset = sub.next_payload(RECV).await;
    assert_eq!(reset["typingDeltas"]["__typename"], "TypingReset");

    ok(&world
        .gql(
            &pass,
            "mutation($b:UUID!,$l:String!){setTyping(board:$b,label:$l){success}}",
            serde_json::json!({ "b": board, "l": "composing" }),
        )
        .await);

    let upsert = loop {
        let delta = sub.next_payload(RECV).await;
        if delta["typingDeltas"]["__typename"] == "TypingUpsert" {
            break delta;
        }
    };
    assert_eq!(upsert["typingDeltas"]["view"]["label"], "composing");

    world.cleanup().await;
}
