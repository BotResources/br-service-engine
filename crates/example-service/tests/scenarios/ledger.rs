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
                "mutation($id:UUID!,$a:Int!){recordEntry(id:$id,amount:$a){success}}",
                serde_json::json!({ "id": l, "a": amount }),
            )
            .await);
    }
    let view = world
        .gql(
            &pass,
            "query($id:UUID!){ledger(id:$id){total}}",
            serde_json::json!({ "id": l }),
        )
        .await;
    assert_eq!(ok(&view)["ledger"]["total"], 8);

    let refused = world
        .gql(
            &pass,
            "mutation($id:UUID!,$a:Int!){recordEntry(id:$id,amount:$a){success}}",
            serde_json::json!({ "id": l, "a": -100 }),
        )
        .await;
    assert_eq!(error_code(&refused), "ledger_would_go_negative");

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
            "mutation($id:UUID!,$a:Int!){recordEntry(id:$id,amount:$a){success}}",
            serde_json::json!({ "id": id, "a": amount }),
        )
        .await);
}

async fn total(world: &World, pass: &str, id: Uuid) -> i64 {
    let view = world
        .gql(
            pass,
            "query($id:UUID!){ledger(id:$id){total}}",
            serde_json::json!({ "id": id }),
        )
        .await;
    ok(&view)["ledger"]["total"].as_i64().expect("a total")
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
    assert_eq!(event_count(&world.db.app, l).await, 8, "eight facts on the log");
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
    assert_eq!(authored, 0, "the author is scrubbed from every event in place");

    assert_eq!(
        total(&world, &pass, l).await,
        7,
        "the rewritten log still hydrates and totals correctly",
    );

    world.cleanup().await;
}
