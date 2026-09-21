use service_engine::erase::PersonId;
use uuid::Uuid;

use crate::harness::{World, error_code, ok, passport};

#[tokio::test]
async fn full_eda_ledger_gate_and_totals() {
    let world = World::start("pod-ledger").await;
    let org = Uuid::now_v7();
    let pass = passport(Uuid::now_v7(), org, &[], false);
    let l = Uuid::now_v7();

    for amount in [5, 3] {
        ok(&world
            .gql(
                &pass,
                "mutation($id:UUID!,$a:Int!){exampleRecordEntry(id:$id,amount:$a){success}}",
                serde_json::json!({ "id": l, "a": amount }),
            )
            .await);
    }
    let view = world
        .gql(
            &pass,
            "query($id:UUID!){exampleLedger(id:$id){total}}",
            serde_json::json!({ "id": l }),
        )
        .await;
    assert_eq!(ok(&view)["exampleLedger"]["total"], 8);

    let refused = world
        .gql(
            &pass,
            "mutation($id:UUID!,$a:Int!){exampleRecordEntry(id:$id,amount:$a){success}}",
            serde_json::json!({ "id": l, "a": -100 }),
        )
        .await;
    assert_eq!(error_code(&refused), "LEDGER_WOULD_GO_NEGATIVE");

    world.cleanup().await;
}

fn key_text(id: Uuid) -> String {
    serde_json::to_string(&id).expect("encode the ledger key as the kit stores it")
}

async fn snapshot_version(pool: &sqlx::PgPool, id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT version FROM service_engine.event_snapshot WHERE noun = 'ledger' AND key = $1",
    )
    .bind(key_text(id))
    .fetch_one(pool)
    .await
    .expect("read the ledger snapshot version")
}

async fn snapshot_last_author(pool: &sqlx::PgPool, id: Uuid) -> Option<Uuid> {
    let raw: Option<String> = sqlx::query_scalar(
        "SELECT state ->> 'last_author' FROM service_engine.event_snapshot \
         WHERE noun = 'ledger' AND key = $1",
    )
    .bind(key_text(id))
    .fetch_one(pool)
    .await
    .expect("read the ledger snapshot state");
    raw.and_then(|value| Uuid::parse_str(&value).ok())
}

async fn event_count(pool: &sqlx::PgPool, id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM service_engine.event_log WHERE noun = 'ledger' AND key = $1",
    )
    .bind(key_text(id))
    .fetch_one(pool)
    .await
    .expect("count the ledger events")
}

async fn record(world: &World, pass: &str, id: Uuid, amount: i64) {
    ok(&world
        .gql(
            pass,
            "mutation($id:UUID!,$a:Int!){exampleRecordEntry(id:$id,amount:$a){success}}",
            serde_json::json!({ "id": id, "a": amount }),
        )
        .await);
}

async fn total(world: &World, pass: &str, id: Uuid) -> i64 {
    let view = world
        .gql(
            pass,
            "query($id:UUID!){exampleLedger(id:$id){total}}",
            serde_json::json!({ "id": id }),
        )
        .await;
    ok(&view)["exampleLedger"]["total"].as_i64().expect("a total")
}

#[tokio::test]
async fn full_eda_snapshot_cadence_writes_once_per_window() {
    let world = World::start("pod-ledger-cadence").await;
    let pass = passport(Uuid::now_v7(), Uuid::now_v7(), &[], false);
    let l = Uuid::now_v7();

    for _ in 0..7 {
        record(&world, &pass, l, 1).await;
    }
    assert_eq!(
        snapshot_version(&world.db.app, l).await,
        0,
        "under the eight-event window the snapshot still sits at the create checkpoint"
    );

    record(&world, &pass, l, 1).await;
    assert_eq!(
        event_count(&world.db.app, l).await,
        8,
        "eight facts on the log"
    );
    assert_eq!(
        snapshot_version(&world.db.app, l).await,
        8,
        "crossing the window writes exactly one snapshot, at the window boundary"
    );
    assert_eq!(total(&world, &pass, l).await, 8);

    world.cleanup().await;
}

#[tokio::test]
async fn full_eda_replay_from_snapshot_equals_replay_from_scratch() {
    let world = World::start("pod-ledger-replay").await;
    let pass = passport(Uuid::now_v7(), Uuid::now_v7(), &[], false);
    let l = Uuid::now_v7();

    let mut sum = 0;
    for amount in 1..=10 {
        record(&world, &pass, l, amount).await;
        sum += amount;
    }
    assert_eq!(
        snapshot_version(&world.db.app, l).await,
        8,
        "the snapshot lags the log, so a load replays from a mid-log checkpoint, not from scratch"
    );
    assert_eq!(
        total(&world, &pass, l).await,
        sum,
        "replay from the lagging snapshot reaches the same total as a replay from the first event"
    );

    world.cleanup().await;
}

#[tokio::test]
async fn full_eda_upcasts_an_old_event_version_at_read() {
    let world = World::start("pod-ledger-upcast").await;
    let pass = passport(Uuid::now_v7(), Uuid::now_v7(), &[], false);
    let l = Uuid::now_v7();

    record(&world, &pass, l, 5).await;

    let by = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO service_engine.event_log (noun, key, seq, version, payload) \
         VALUES ('ledger', $1, 2, 1, $2)",
    )
    .bind(key_text(l))
    .bind(serde_json::json!({ "kind": "Recorded", "delta": 7, "by": by.to_string() }))
    .execute(&world.db.app)
    .await
    .expect("append a v1-shaped event straight onto the log");

    assert_eq!(
        total(&world, &pass, l).await,
        12,
        "the old event shape is upcast at read and folds into the total",
    );

    world.cleanup().await;
}

#[tokio::test]
async fn full_eda_erasure_leaves_the_log_readable() {
    let world = World::start("pod-ledger-erase").await;
    let user = Uuid::now_v7();
    let pass = passport(user, Uuid::now_v7(), &[], false);
    let l = Uuid::now_v7();

    record(&world, &pass, l, 3).await;
    record(&world, &pass, l, 4).await;

    world
        .service
        .erase(PersonId(user))
        .await
        .expect("erase the author");

    let authored: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM service_engine.event_log \
         WHERE noun = 'ledger' AND payload ->> 'author' = $1",
    )
    .bind(user.to_string())
    .fetch_one(&world.db.app)
    .await
    .unwrap();
    assert_eq!(
        authored, 0,
        "the author is scrubbed from every event in place"
    );

    assert_eq!(
        total(&world, &pass, l).await,
        7,
        "the rewritten log still hydrates and totals correctly",
    );

    world.cleanup().await;
}

#[tokio::test]
async fn full_eda_erasure_scrubs_a_crossed_snapshot_boundary() {
    let world = World::start("pod-ledger-erase-snapshot").await;
    let org = Uuid::now_v7();
    let other = Uuid::now_v7();
    let person = Uuid::now_v7();
    let other_pass = passport(other, org, &[], false);
    let person_pass = passport(person, org, &[], false);
    let l = Uuid::now_v7();

    for _ in 0..7 {
        record(&world, &other_pass, l, 1).await;
    }
    record(&world, &person_pass, l, 1).await;

    assert_eq!(
        snapshot_version(&world.db.app, l).await,
        8,
        "the eighth fact crosses the window and rewrites the snapshot with the person folded in"
    );
    assert_eq!(
        snapshot_last_author(&world.db.app, l).await,
        Some(person),
        "the lagging snapshot has folded the person into its state before erasure"
    );

    world
        .service
        .erase(PersonId(person))
        .await
        .expect("erase the author");

    let residual: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM service_engine.event_log \
         WHERE noun = 'ledger' AND payload ->> 'author' = $1",
    )
    .bind(person.to_string())
    .fetch_one(&world.db.app)
    .await
    .unwrap();
    assert_eq!(residual, 0, "no event on the log still carries the person");

    assert_eq!(
        snapshot_last_author(&world.db.app, l).await,
        Some(Uuid::nil()),
        "the re-snapshot rebuilt from the whole rewritten log, so the snapshot row keeps no residual person past a crossed boundary"
    );

    let view = world
        .gql(
            &other_pass,
            "query($id:UUID!){exampleLedger(id:$id){total lastAuthor}}",
            serde_json::json!({ "id": l }),
        )
        .await;
    let ledger = &ok(&view)["exampleLedger"];
    assert_eq!(
        ledger["total"], 8,
        "the rewritten log still hydrates and totals correctly"
    );
    assert_eq!(
        ledger["lastAuthor"],
        Uuid::nil().to_string(),
        "the read side exposes no residual person after erasure"
    );

    world.cleanup().await;
}
