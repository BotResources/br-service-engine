use example_contract::{CreateCard, PersonCreated, PublishedPerson};
use service_engine::nats::Nats;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let nats = Nats::connect(&nats_url).await?;

    let board_id = std::env::var("BOARD_ID")
        .ok()
        .and_then(|raw| Uuid::parse_str(&raw).ok())
        .unwrap_or_else(Uuid::now_v7);
    let person_id = Uuid::now_v7();
    let card_id = Uuid::now_v7();

    let person = PublishedPerson {
        id: person_id,
        email: format!("{}@example.test", person_id.simple()),
        display_name: "Twin Person".to_string(),
    };
    example_twin::publish_person(&nats, &person).await?;
    example_twin::send_person_created(
        &nats,
        &PersonCreated {
            person_id,
            board_id,
            display_name: person.display_name.clone(),
        },
    )
    .await?;
    example_twin::send_create_card(
        &nats,
        &CreateCard {
            card_id,
            board_id,
            title: "Card requested by the twin".to_string(),
        },
    )
    .await?;

    if let Some(ready) = example_twin::next_card_ready(&nats).await? {
        println!("twin saw the main service confirm card {}", ready.card_id);
    }
    Ok(())
}
