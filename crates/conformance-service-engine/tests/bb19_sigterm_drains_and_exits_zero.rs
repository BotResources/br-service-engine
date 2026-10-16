mod blackbox_support;

use std::time::Duration;

use blackbox_support::bin::{read_log, wait_exit};
use blackbox_support::signal::send_signal;
use blackbox_support::{GraphqlWs, World, ok, passport};
use uuid::Uuid;

const BOARD_ARCHIVE: &str = "example:board_archive";
const RECV: Duration = Duration::from_secs(15);
const CLOSE_WITHIN: Duration = Duration::from_secs(15);
const EXIT_WITHIN: Duration = Duration::from_secs(30);
const SUB: &str = "subscription{exampleBoardDeltas{__typename \
    ... on BoardReset{revision views{... on BoardView{id archived}}} \
    ... on BoardUpsert{revision view{... on BoardView{id archived}}} \
    ... on BoardRemove{revision}}}";

#[tokio::test]
async fn bb19_a_sigterm_to_the_serve_binary_ends_every_session_and_exits_zero() {
    let mut world = World::start("bb19-pod").await;

    let pass = passport(Uuid::now_v7(), Uuid::now_v7(), &[BOARD_ARCHIVE], false);
    ok(&world
        .gql(
            &pass,
            "mutation($id:UUID!,$n:String!){exampleCreateBoard(id:$id,name:$n,isPublic:false){success}}",
            serde_json::json!({ "id": Uuid::now_v7(), "n": "Drained board" }),
        )
        .await);
    let mut ws = GraphqlWs::connect(world.base_url(), &pass).await;
    ws.subscribe("1", SUB).await;
    let reset = ws
        .next_data(RECV)
        .await
        .expect("the session attaches and delivers a Reset");
    assert_eq!(reset["exampleBoardDeltas"]["__typename"], "BoardReset");

    send_signal(world.service.pid(), "TERM");

    ws.expect_close(CLOSE_WITHIN).await;
    let status = wait_exit(world.service.child_mut(), EXIT_WITHIN)
        .await
        .expect("the binary exits by itself after SIGTERM instead of being killed");
    assert!(
        status.success(),
        "a SIGTERM is a graceful shutdown, so the process exits 0, got {status}"
    );
    let log = read_log(world.service.log_path());
    assert!(
        log.contains("process signal received") && log.contains("SIGTERM"),
        "the binary logs the signal that started the shutdown, got:\n{log}"
    );

    world.cleanup().await;
}
