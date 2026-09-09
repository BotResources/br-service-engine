mod harness;

use std::time::Duration;

use example_contract::{CreateCard, PublishedBoard, PublishedPerson};
use example_service::slices::reply::ReplyText;
use example_service::twin;
use harness::{World, error_code, ok, passport, settle};
use service_engine::accumulator::ChunkSeq;
use service_engine::nats::KvKey;
use uuid::Uuid;

const BOARD_ARCHIVE: &str = "example.board.archive";
const CARD_ADVANCE: &str = "example.card.advance";

#[tokio::test]
async fn board_direct_lane_gate_affordance_offer_visibility_oneshot() {
    let world = World::start("pod-board").await;
    let org1 = Uuid::now_v7();
    let user1 = Uuid::now_v7();
    let owner = passport(user1, org1, &[BOARD_ARCHIVE], false);

    let b1 = Uuid::now_v7();
    let created = world
        .gql(
            &owner,
            "mutation($id:UUID!,$n:String!){createBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": b1, "n": "First" }),
        )
        .await;
    assert_eq!(ok(&created)["createBoard"]["success"], true);

    let view = world
        .gql(
            &owner,
            "query($id:UUID!){board(id:$id){id archived affordances}}",
            serde_json::json!({ "id": b1 }),
        )
        .await;
    let board = &ok(&view)["board"];
    assert_eq!(board["archived"], false);
    assert_eq!(board["affordances"]["archive"]["allowed"], true);

    let archived = world
        .gql(
            &owner,
            "mutation($id:UUID!){archiveBoard(id:$id){success}}",
            serde_json::json!({ "id": b1 }),
        )
        .await;
    assert_eq!(ok(&archived)["archiveBoard"]["success"], true);

    let after = world
        .gql(
            &owner,
            "query($id:UUID!){board(id:$id){archived affordances}}",
            serde_json::json!({ "id": b1 }),
        )
        .await;
    assert_eq!(ok(&after)["board"]["archived"], true);
    assert_eq!(
        ok(&after)["board"]["affordances"]["archive"]["allowed"],
        false
    );

    let refused = world
        .gql(
            &owner,
            "mutation($id:UUID!){archiveBoard(id:$id){success}}",
            serde_json::json!({ "id": b1 }),
        )
        .await;
    assert_eq!(error_code(&refused), "board_not_active");

    let b2 = Uuid::now_v7();
    ok(&world
        .gql(
            &owner,
            "mutation($id:UUID!,$n:String!){createBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": b2, "n": "Second" }),
        )
        .await);

    let invite = world
        .gql(
            &owner,
            "mutation($id:UUID!){mintBoardInvite(id:$id)}",
            serde_json::json!({ "id": b2 }),
        )
        .await;
    assert!(
        ok(&invite)["mintBoardInvite"]
            .as_str()
            .unwrap()
            .starts_with("invite-")
    );

    settle().await;
    let bucket = world
        .nats
        .published_language::<PublishedBoard>()
        .await
        .unwrap();
    let key = KvKey::new(format!("example/boards/v1/{b2}")).unwrap();
    let published = bucket.get(&key).await.unwrap();
    assert_eq!(published.map(|p| p.name), Some("Second".to_string()));

    let b3 = Uuid::now_v7();
    ok(&world
        .gql(
            &owner,
            "mutation($id:UUID!,$n:String!){createBoard(id:$id,name:$n,isPublic:true){success}}",
            serde_json::json!({ "id": b3, "n": "Public" }),
        )
        .await);

    let outsider = passport(Uuid::now_v7(), Uuid::now_v7(), &[], false);
    let visible = world
        .gql(&outsider, "query{boards{id}}", serde_json::json!({}))
        .await;
    let ids: Vec<String> = ok(&visible)["boards"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["id"].as_str().unwrap().to_string())
        .collect();
    assert!(
        ids.contains(&b3.to_string()),
        "an outsider sees the public board"
    );
    assert!(
        !ids.contains(&b2.to_string()),
        "an outsider never sees another org's private board"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn cross_service_cycle_mirror_and_soft_eda() {
    let world = World::start("pod-cycle").await;
    let org = Uuid::now_v7();
    let user = Uuid::now_v7();
    let pass = passport(user, org, &[CARD_ADVANCE], false);
    let board = Uuid::now_v7();

    let person = PublishedPerson {
        id: Uuid::now_v7(),
        email: "dana@example.test".to_string(),
        display_name: "Dana".to_string(),
    };
    twin::publish_person(&world.nats, &person).await.unwrap();
    settle().await;
    let roster = world
        .gql(
            &pass,
            "query($id:UUID!){person(id:$id){email displayName}}",
            serde_json::json!({ "id": person.id }),
        )
        .await;
    assert_eq!(ok(&roster)["person"]["displayName"], "Dana");

    let card_id = Uuid::now_v7();
    twin::send_create_card(
        &world.nats,
        &CreateCard {
            card_id,
            board_id: board,
            title: "From twin".to_string(),
        },
    )
    .await
    .unwrap();
    for _ in 0..30 {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM card WHERE id = $1")
            .bind(card_id)
            .fetch_one(&world.db.app)
            .await
            .unwrap();
        if n > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let card = world
        .gql(
            &pass,
            "query($id:UUID!){card(id:$id){title status}}",
            serde_json::json!({ "id": card_id }),
        )
        .await;
    assert_eq!(ok(&card)["card"]["title"], "From twin");
    assert_eq!(ok(&card)["card"]["status"], "todo");

    let mut confirmed = None;
    for _ in 0..30 {
        if let Some(ready) = twin::card_ready_from_stream(&world.nats).await.unwrap() {
            confirmed = Some(ready);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        confirmed.map(|c| c.card_id),
        Some(card_id),
        "the main service emitted CardReady back over NATS (the return leg of the cycle)"
    );

    let advanced = world
        .gql(
            &pass,
            "mutation($id:UUID!){advanceCard(id:$id){success}}",
            serde_json::json!({ "id": card_id }),
        )
        .await;
    assert_eq!(ok(&advanced)["advanceCard"]["success"], true);
    settle().await;
    let card = world
        .gql(
            &pass,
            "query($id:UUID!){card(id:$id){status}}",
            serde_json::json!({ "id": card_id }),
        )
        .await;
    assert_eq!(ok(&card)["card"]["status"], "doing");

    let facts: i64 = sqlx::query_scalar("SELECT count(*) FROM card_fact WHERE card = $1")
        .bind(card_id)
        .fetch_one(&world.db.app)
        .await
        .unwrap();
    assert!(
        facts >= 2,
        "soft EDA appended one fact per change, got {facts}"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn full_eda_ledger_gate_and_totals() {
    let world = World::start("pod-ledger").await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let l = Uuid::now_v7();

    for amount in [5, 3] {
        ok(&world
            .gql(
                &pass,
                "mutation($id:UUID!,$a:Int!){recordEntry(id:$id,amount:$a){success}}",
                serde_json::json!({ "id": l, "a": amount }),
            )
            .await);
    }
    settle().await;
    let view = world
        .gql(
            &pass,
            "query($id:UUID!){ledger(id:$id){total}}",
            serde_json::json!({ "id": l }),
        )
        .await;
    assert_eq!(ok(&view)["ledger"]["total"], 8);

    let refused = world
        .gql(
            &pass,
            "mutation($id:UUID!,$a:Int!){recordEntry(id:$id,amount:$a){success}}",
            serde_json::json!({ "id": l, "a": -100 }),
        )
        .await;
    assert_eq!(error_code(&refused), "ledger_would_go_negative");

    world.cleanup().await;
}

#[tokio::test]
async fn accumulated_lane_seals_the_streamed_reply() {
    let world = World::start("pod-reply").await;
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

    for (seq, chunk) in [(0u64, "Hel"), (1, "lo "), (2, "world")] {
        world
            .service
            .chunks
            .push_chunk::<ReplyText>(&reply, ChunkSeq::new(seq).unwrap(), chunk.to_string())
            .unwrap()
            .await
            .unwrap();
    }

    twin::send_reply_finished(&world.nats, reply, board)
        .await
        .unwrap();
    settle().await;
    settle().await;

    let view = world
        .gql(
            &pass,
            "query($id:UUID!){reply(id:$id){text status}}",
            serde_json::json!({ "id": reply }),
        )
        .await;
    assert_eq!(ok(&view)["reply"]["status"], "complete");
    assert_eq!(ok(&view)["reply"]["text"], "Hello world");

    world.cleanup().await;
}

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
    settle().await;

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
    settle().await;

    let bucket = world
        .nats
        .bind_ephemeral::<serde_json::Value>(&format!("EPHEMERAL_{}", example_contract::SERVICE))
        .await
        .unwrap();
    let key = KvKey::new(format!("typing/{}/{}", board.simple(), user.simple())).unwrap();
    let value = bucket.get(&key).await.unwrap();
    assert_eq!(
        value.and_then(|v| v["label"].as_str().map(str::to_string)),
        Some("drafting".to_string())
    );

    world.cleanup().await;
}
