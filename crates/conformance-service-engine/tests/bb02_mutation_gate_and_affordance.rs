mod blackbox_support;

use blackbox_support::{World, error_code, ok, passport};
use uuid::Uuid;

const BOARD_ARCHIVE: &str = "example:board_archive";

#[tokio::test]
async fn bb02_the_mutation_gate_refuses_exactly_what_the_affordance_forbids() {
    let world = World::start("bb02-pod").await;
    let org = Uuid::now_v7();
    let user = Uuid::now_v7();
    let owner = passport(user, org, &[BOARD_ARCHIVE], false);
    let board = Uuid::now_v7();

    ok(&world
        .gql(
            &owner,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": board, "n": "Black-box board" }),
        )
        .await);

    let before = world
        .gql(
            &owner,
            "query($id:UUID!){exampleBoard(id:$id){archived affordances}}",
            serde_json::json!({ "id": board }),
        )
        .await;
    assert_eq!(ok(&before)["exampleBoard"]["archived"], false);
    assert_eq!(
        ok(&before)["exampleBoard"]["affordances"]["archive"]["allowed"],
        true,
        "before archiving, the archive affordance is allowed"
    );

    ok(&world
        .gql(
            &owner,
            "mutation($id:UUID!){exampleArchiveBoard(id:$id){success}}",
            serde_json::json!({ "id": board }),
        )
        .await);

    let after = world
        .gql(
            &owner,
            "query($id:UUID!){exampleBoard(id:$id){archived affordances}}",
            serde_json::json!({ "id": board }),
        )
        .await;
    assert_eq!(ok(&after)["exampleBoard"]["archived"], true);
    assert_eq!(
        ok(&after)["exampleBoard"]["affordances"]["archive"]["allowed"],
        false,
        "after archiving, the same affordance flips to forbidden"
    );

    let refused = world
        .gql(
            &owner,
            "mutation($id:UUID!){exampleArchiveBoard(id:$id){success}}",
            serde_json::json!({ "id": board }),
        )
        .await;
    assert_eq!(
        error_code(&refused),
        "BOARD_NOT_ACTIVE",
        "the mutation the affordance forbids is refused by the same gate, with a typed code"
    );

    world.cleanup().await;
}
