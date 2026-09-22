mod blackbox_support;

use std::time::{Duration, Instant};

use async_nats::jetstream::consumer::pull;
use blackbox_support::World;
use br_core_integration::IntegrationEvent;
use example_contract::{CardReady, CreateCard, card_ready_coords};
use futures_util::StreamExt;
use service_engine::nats::{Nats, event_subject};
use sqlx::PgPool;
use uuid::Uuid;

const WITHIN: Duration = Duration::from_secs(25);
const POLL: Duration = Duration::from_millis(150);

async fn card_ready_on_the_wire(nats: &Nats, board: Uuid) -> usize {
    let subject = event_subject(&card_ready_coords());
    let stream = nats
        .context()
        .get_stream("INTEGRATION_EVT")
        .await
        .expect("the integration event stream exists");
    let consumer = stream
        .create_consumer(pull::Config {
            filter_subject: subject,
            ..Default::default()
        })
        .await
        .expect("a pull consumer binds the card.ready subject");
    let mut batch = consumer
        .fetch()
        .max_messages(64)
        .messages()
        .await
        .expect("fetch the card.ready backlog");
    let mut count = 0;
    while let Some(message) = batch.next().await {
        let message = message.expect("a jetstream message");
        if let Ok(envelope) =
            serde_json::from_slice::<IntegrationEvent<CardReady>>(&message.payload)
            && envelope.payload.board_id == board
        {
            count += 1;
        }
        let _ = message.ack().await;
    }
    count
}

async fn scalar_by_subject(pool: &PgPool, sql: &str, subject: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(subject)
        .fetch_one(pool)
        .await
        .expect("read the outbox")
}

#[tokio::test]
async fn bb10_an_outbox_row_staged_on_one_pod_is_published_once_though_both_pods_relay() {
    let world = World::start("bb10-pod-a").await;
    let pod_b = world.spawn_pod("bb10-pod-b").await;

    let board = Uuid::now_v7();
    let card = Uuid::now_v7();

    example_twin::send_create_card(
        &world.nats,
        &CreateCard {
            card_id: card,
            board_id: board,
            title: "shared card".to_string(),
        },
    )
    .await
    .expect("send the CreateCard command");

    let deadline = Instant::now() + WITHIN;
    loop {
        let cards: i64 = sqlx::query_scalar("SELECT count(*) FROM card WHERE id = $1")
            .bind(card)
            .fetch_one(&world.db.app)
            .await
            .expect("read the card table");
        if cards == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the CreateCard command was never committed by exactly one pod (cards = {cards})"
        );
        tokio::time::sleep(POLL).await;
    }

    let subject = event_subject(&card_ready_coords());
    let deadline = Instant::now() + WITHIN;
    loop {
        let published = scalar_by_subject(
            &world.db.app,
            "SELECT count(*) FROM service_engine.integration_outbox WHERE subject = $1 AND status = 'PUBLISHED'",
            &subject,
        )
        .await;
        if published >= 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the outbox row was never drained to PUBLISHED by either pod's relay"
        );
        tokio::time::sleep(POLL).await;
    }

    assert_eq!(
        scalar_by_subject(
            &world.db.app,
            "SELECT count(*) FROM service_engine.integration_outbox WHERE subject = $1",
            &subject,
        )
        .await,
        1,
        "one command staged exactly one CardReady outbox row"
    );
    assert_eq!(
        scalar_by_subject(
            &world.db.app,
            "SELECT count(*) FROM service_engine.integration_outbox WHERE subject = $1 AND status = 'PUBLISHED'",
            &subject,
        )
        .await,
        1,
        "the row reached PUBLISHED once, not once per relaying pod"
    );

    assert_eq!(
        card_ready_on_the_wire(&world.nats, board).await,
        1,
        "the outbox row was published once even though pod B was also draining the outbox"
    );

    pod_b.shutdown().await;
    world.cleanup().await;
}
