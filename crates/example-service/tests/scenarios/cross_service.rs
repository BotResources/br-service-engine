use std::time::Duration;

use example_contract::{CreateCard, PublishedPerson};
use uuid::Uuid;

use crate::harness::{World, ok, passport};
use crate::poll_until;

const CARD_ADVANCE: &str = "example:card_advance";

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
    example_twin::publish_person(&world.nats, &person)
        .await
        .unwrap();
    let display_name: String = poll_until!(Duration::from_secs(5), {
        let roster = world
            .gql(
                &pass,
                "query($id:UUID!){examplePerson(id:$id){email displayName}}",
                serde_json::json!({ "id": person.id }),
            )
            .await;
        roster["data"]["examplePerson"]["displayName"]
            .as_str()
            .map(str::to_string)
    });
    assert_eq!(display_name, "Dana");

    let card_id = Uuid::now_v7();
    example_twin::send_create_card(
        &world.nats,
        &CreateCard {
            card_id,
            board_id: board,
            title: "From twin".to_string(),
        },
    )
    .await
    .unwrap();
    poll_until!(Duration::from_secs(5), {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM card WHERE id = $1")
            .bind(card_id)
            .fetch_one(&world.db.app)
            .await
            .unwrap();
        (n > 0).then_some(())
    });

    let card = world
        .gql(
            &pass,
            "query($id:UUID!){exampleCard(id:$id){title status}}",
            serde_json::json!({ "id": card_id }),
        )
        .await;
    assert_eq!(ok(&card)["exampleCard"]["title"], "From twin");
    assert_eq!(ok(&card)["exampleCard"]["status"], "todo");

    let confirmed = poll_until!(Duration::from_secs(5), {
        example_twin::card_ready_from_stream(&world.nats)
            .await
            .unwrap()
    });
    assert_eq!(
        confirmed.card_id, card_id,
        "the main service emitted CardReady back over NATS (the return leg of the cycle)"
    );

    let advanced = world
        .gql(
            &pass,
            "mutation($id:UUID!){exampleAdvanceCard(id:$id){success}}",
            serde_json::json!({ "id": card_id }),
        )
        .await;
    assert_eq!(ok(&advanced)["exampleAdvanceCard"]["success"], true);
    let card = world
        .gql(
            &pass,
            "query($id:UUID!){exampleCard(id:$id){status}}",
            serde_json::json!({ "id": card_id }),
        )
        .await;
    assert_eq!(ok(&card)["exampleCard"]["status"], "doing");

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
