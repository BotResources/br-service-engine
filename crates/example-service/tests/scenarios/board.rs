use std::collections::BTreeSet;
use std::time::Duration;

use example_contract::PublishedBoard;
use service_engine::nats::KvKey;
use uuid::Uuid;

use crate::harness::{World, error_code, ok, passport};
use crate::poll_until;

const BOARD_ARCHIVE: &str = "example:board_archive";

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
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": b1, "n": "First" }),
        )
        .await;
    assert_eq!(ok(&created)["exampleCreateBoard"]["success"], true);

    let view = world
        .gql(
            &owner,
            "query($id:UUID!){exampleBoard(id:$id){id archived affordances}}",
            serde_json::json!({ "id": b1 }),
        )
        .await;
    let board = &ok(&view)["exampleBoard"];
    assert_eq!(board["archived"], false);
    assert_eq!(board["affordances"]["archive"]["allowed"], true);

    let archived = world
        .gql(
            &owner,
            "mutation($id:UUID!){exampleArchiveBoard(id:$id){success}}",
            serde_json::json!({ "id": b1 }),
        )
        .await;
    assert_eq!(ok(&archived)["exampleArchiveBoard"]["success"], true);

    let after = world
        .gql(
            &owner,
            "query($id:UUID!){exampleBoard(id:$id){archived affordances}}",
            serde_json::json!({ "id": b1 }),
        )
        .await;
    assert_eq!(ok(&after)["exampleBoard"]["archived"], true);
    assert_eq!(
        ok(&after)["exampleBoard"]["affordances"]["archive"]["allowed"],
        false
    );

    let refused = world
        .gql(
            &owner,
            "mutation($id:UUID!){exampleArchiveBoard(id:$id){success}}",
            serde_json::json!({ "id": b1 }),
        )
        .await;
    assert_eq!(error_code(&refused), "BOARD_NOT_ACTIVE");

    let b2 = Uuid::now_v7();
    ok(&world
        .gql(
            &owner,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": b2, "n": "Second" }),
        )
        .await);

    let invite = world
        .gql(
            &owner,
            "mutation($id:UUID!){exampleMintBoardInvite(id:$id)}",
            serde_json::json!({ "id": b2 }),
        )
        .await;
    assert!(
        ok(&invite)["exampleMintBoardInvite"]
            .as_str()
            .unwrap()
            .starts_with("invite-")
    );

    let bucket = world
        .nats
        .published_language::<PublishedBoard>()
        .await
        .unwrap();
    let key = KvKey::new(format!("example/boards/v1/{b2}")).unwrap();
    let published = poll_until!(Duration::from_secs(5), { bucket.get(&key).await.unwrap() });
    assert_eq!(published.name, "Second".to_string());

    let b3 = Uuid::now_v7();
    ok(&world
        .gql(
            &owner,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:true){success}}",
            serde_json::json!({ "id": b3, "n": "Public" }),
        )
        .await);

    let outsider = passport(Uuid::now_v7(), Uuid::now_v7(), &[], false);
    let visible = world
        .gql(&outsider, "query{exampleBoards{id}}", serde_json::json!({}))
        .await;
    let ids: Vec<String> = ok(&visible)["exampleBoards"]
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

async fn create_board(world: &World, owner: &str, name: &str, is_public: bool) -> Uuid {
    let id = Uuid::now_v7();
    ok(&world
        .gql(
            owner,
            "mutation($id:UUID!,$n:String!,$p:Boolean!){exampleCreateBoard(id:$id,name:$n,isPublic:$p){success}}",
            serde_json::json!({ "id": id, "n": name, "p": is_public }),
        )
        .await);
    id
}

async fn listed_boards(world: &World, viewer: &str) -> BTreeSet<Uuid> {
    let listed = world
        .gql(viewer, "query{exampleBoards{id}}", serde_json::json!({}))
        .await;
    ok(&listed)["exampleBoards"]
        .as_array()
        .unwrap_or_else(|| panic!("exampleBoards answers a list: {listed}"))
        .iter()
        .map(|view| {
            view["id"]
                .as_str()
                .and_then(|id| Uuid::parse_str(id).ok())
                .expect("a board view carries its id")
        })
        .collect()
}

#[tokio::test]
async fn the_boards_list_holds_exactly_the_org_public_and_member_cohorts() {
    let world = World::start("pod-board-cohorts").await;
    let home = Uuid::now_v7();
    let away = Uuid::now_v7();
    let viewer_id = Uuid::now_v7();
    let colleague = passport(Uuid::now_v7(), home, &[], false);
    let stranger = passport(Uuid::now_v7(), away, &[], false);
    let viewer = passport(viewer_id, home, &[], false);

    let mine = create_board(&world, &colleague, "Mine", false).await;
    let public = create_board(&world, &stranger, "Public", true).await;
    let shared = create_board(&world, &stranger, "Shared", false).await;
    let hidden = create_board(&world, &stranger, "Hidden", false).await;
    ok(&world
        .gql(
            &stranger,
            "mutation($b:UUID!,$u:UUID!){exampleSetBoardMembership(boardId:$b,userId:$u,member:true){success}}",
            serde_json::json!({ "b": shared, "u": viewer_id }),
        )
        .await);

    let listed = listed_boards(&world, &viewer).await;
    assert_eq!(
        listed,
        BTreeSet::from([mine, public, shared]),
        "the list is the org cohort, the public cohort and the member cohort, read by the store"
    );
    assert!(
        !listed.contains(&hidden),
        "a private board of another org the viewer is not a member of is never listed"
    );

    world.cleanup().await;
}
