mod blackbox_support;

use std::time::{Duration, Instant};

use blackbox_support::World;
use blackbox_support::bin::spawn_twin;
use uuid::Uuid;

#[tokio::test]
async fn bb04_the_example_twin_binary_drives_a_full_cross_service_cycle_against_the_binary() {
    let world = World::start("bb04-pod").await;
    let board = Uuid::now_v7();

    let _twin = spawn_twin(&world.nats_server.url(), board);

    let deadline = Instant::now() + Duration::from_secs(25);
    loop {
        let cards: i64 = sqlx::query_scalar("SELECT count(*) FROM card WHERE board_id = $1")
            .bind(board)
            .fetch_one(&world.db.app)
            .await
            .expect("read the card table");
        if cards >= 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the spawned twin binary's CreateCard command was never committed by the running service"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    let deadline = Instant::now() + Duration::from_secs(25);
    let confirmed = loop {
        if let Some(ready) = example_twin::card_ready_from_stream(&world.nats)
            .await
            .expect("read the integration event stream")
        {
            break ready;
        }
        assert!(
            Instant::now() < deadline,
            "the running service never emitted CardReady back onto NATS (the return leg of the cycle)"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(
        confirmed.board_id, board,
        "the CardReady the running binary published names the board the twin targeted"
    );

    world.cleanup().await;
}
