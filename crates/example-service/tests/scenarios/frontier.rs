use std::time::Duration;

use example_contract::{CreateCard, PersonCreated};
use uuid::Uuid;

use crate::harness::World;
use crate::poll_until;

async fn card_count_on_board(world: &World, board: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM card WHERE board_id = $1")
        .bind(board)
        .fetch_one(&world.db.app)
        .await
        .unwrap()
}

async fn dead_letters_for(world: &World, message_id: Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.dead_letter WHERE message_id = $1")
        .bind(message_id)
        .fetch_one(&world.db.app)
        .await
        .unwrap()
}

async fn total_dead_letters(world: &World) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM service_engine.dead_letter")
        .fetch_one(&world.db.app)
        .await
        .unwrap()
}

#[tokio::test]
async fn a_fabric_shaped_producer_completes_a_full_cycle_through_the_envelope() {
    let world = World::start("pod-frontier").await;
    let board = Uuid::now_v7();
    let card_id = Uuid::now_v7();

    example_twin::send_create_card(
        &world.nats,
        &CreateCard {
            card_id,
            board_id: board,
            title: "Enveloped".to_string(),
        },
    )
    .await
    .unwrap();

    poll_until!(Duration::from_secs(5), {
        (card_count_on_board(&world, board).await > 0).then_some(())
    });

    let read = poll_until!(Duration::from_secs(5), {
        example_twin::card_ready_envelope_from_stream(&world.nats)
            .await
            .unwrap()
    });

    assert_eq!(
        read.envelope.payload.card_id, card_id,
        "the engine's emitted event decodes with br-core-integration on the twin side"
    );
    assert!(
        read.has_producer_sequence,
        "the engine renders the three Br-* producer-sequence headers on the emitted event"
    );
    assert!(
        read.envelope.metadata.actor.is_service(),
        "a reaction emits under the engine's own service identity"
    );
    assert_eq!(
        read.envelope.metadata.causation_id,
        Some(card_id),
        "the emitted event is caused by the inbound command's envelope id"
    );
    assert_eq!(
        read.envelope.metadata.correlation_id, card_id,
        "the correlation id is propagated from the inbound command"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn an_out_of_order_event_is_a_stale_no_op_not_a_regression() {
    let world = World::start("pod-order").await;
    let board = Uuid::now_v7();
    let person_a = Uuid::now_v7();
    let person_b = Uuid::now_v7();

    let event_a = PersonCreated {
        person_id: person_a,
        board_id: board,
        display_name: "Ada".to_string(),
    };
    example_twin::send_person_created_seq(&world.nats, &event_a, 2)
        .await
        .unwrap();
    poll_until!(Duration::from_secs(5), {
        (card_count_on_board(&world, board).await >= 1).then_some(())
    });

    example_twin::send_person_created_seq(&world.nats, &event_a, 1)
        .await
        .unwrap();

    let event_b = PersonCreated {
        person_id: person_b,
        board_id: board,
        display_name: "Grace".to_string(),
    };
    example_twin::send_person_created_seq(&world.nats, &event_b, 1)
        .await
        .unwrap();

    poll_until!(Duration::from_secs(5), {
        (card_count_on_board(&world, board).await >= 2).then_some(())
    });

    assert_eq!(
        card_count_on_board(&world, board).await,
        2,
        "the stale seq=1 for person A was dropped as a no-op, so no duplicate welcome card exists"
    );
    assert_eq!(
        total_dead_letters(&world).await,
        0,
        "a stale producer sequence is an acked no-op, never a poison message"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn create_card_refuses_a_command_from_a_non_service_sender() {
    let world = World::start("pod-gate").await;
    let board = Uuid::now_v7();
    let card_id = Uuid::now_v7();

    example_twin::send_create_card_as(
        &world.nats,
        &CreateCard {
            card_id,
            board_id: board,
            title: "From a human".to_string(),
        },
        example_twin::human_actor(),
    )
    .await
    .unwrap();

    poll_until!(Duration::from_secs(5), {
        (dead_letters_for(&world, card_id).await == 1).then_some(())
    });
    assert_eq!(
        card_count_on_board(&world, board).await,
        0,
        "a command whose envelope actor is not a service is refused, so no card is created"
    );

    world.cleanup().await;
}
