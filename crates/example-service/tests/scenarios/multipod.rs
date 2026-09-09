use uuid::Uuid;

use crate::harness::{World, ok, passport};

const BOARD_ARCHIVE: &str = "example:board_archive";

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
