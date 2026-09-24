use std::time::Duration;

use example_contract::PublishedPerson;
use uuid::Uuid;

use crate::harness::{World, WorldOptions, ok, passport};
use crate::poll_until;

const CAPACITY: usize = 2;
const PERSON: &str = "query($id:UUID!){examplePerson(id:$id){email displayName}}";

#[tokio::test]
async fn a_person_is_fetched_by_key_on_a_roster_over_window_capacity() {
    let world = World::start_tweaked(
        "pod-roster-person",
        WorldOptions {
            blobs: false,
            declare_scopes: false,
        },
        |config| config.with_window_capacity(CAPACITY),
    )
    .await;
    let viewer = passport(Uuid::now_v7(), Uuid::now_v7(), &[], false);
    let mut people = Vec::with_capacity(CAPACITY + 2);
    for n in 0..CAPACITY + 2 {
        let person = PublishedPerson {
            id: Uuid::now_v7(),
            email: format!("p{n}@example.test"),
            display_name: format!("P{n}"),
        };
        example_twin::publish_person(&world.nats, &person)
            .await
            .unwrap();
        people.push(person);
    }
    let mirrored = i64::try_from(CAPACITY + 3).unwrap();
    poll_until!(Duration::from_secs(5), {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM roster.known_persons")
            .fetch_one(&world.db.app)
            .await
            .unwrap();
        (n == mirrored).then_some(())
    });

    let wanted = &people[1];
    let answer = world
        .gql(&viewer, PERSON, serde_json::json!({ "id": wanted.id }))
        .await;
    assert_eq!(
        ok(&answer)["examplePerson"],
        serde_json::json!({ "email": wanted.email, "displayName": wanted.display_name }),
        "a person of a roster over window_capacity is read by its key, never through the \
         roster's population"
    );

    let absent = world
        .gql(&viewer, PERSON, serde_json::json!({ "id": Uuid::now_v7() }))
        .await;
    assert!(
        ok(&absent)["examplePerson"].is_null(),
        "an unknown person answers null: {absent}"
    );

    world.cleanup().await;
}
