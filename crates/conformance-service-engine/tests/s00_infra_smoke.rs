use br_core_integration::{EventCoords, EventMetadata, IntegrationEvent};
use br_core_kernel::{Actor, UserId};
use br_util_nats_fabric::{Aggregate, Bc, PastFact, PublishedLanguageReader};
use chrono::Utc;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample;
use conformance_service_engine::sample::{SamplePrincipal, SampleRls};
use serde::{Deserialize, Serialize};
use service_engine::principal::RlsApplier;
use sqlx::Row;
use uuid::Uuid;

const DIRECTORY_TABLES: &[&str] = &[
    "known_users",
    "known_groups",
    "known_user_group",
    "known_service_accounts",
];

#[derive(Serialize, Deserialize)]
struct Ping {
    label: String,
}

#[tokio::test]
async fn s00_infra_smoke() {
    let db = TestDb::fresh().await;

    for table in service_engine::schema::TABLES {
        let (schema, name) = table.split_once('.').expect("a qualified engine table");
        assert!(
            table_exists(db.owner_pool(), schema, name).await,
            "{table} is missing after the engine migration set"
        );
    }
    for table in DIRECTORY_TABLES {
        assert!(
            table_exists(db.owner_pool(), "public", table).await,
            "{table} is missing after the directory migration set"
        );
    }
    for table in sample::TABLES {
        assert!(
            table_exists(db.owner_pool(), "public", table).await,
            "{table} is missing after the sample service migration set"
        );
    }

    let versions: Vec<i64> = sqlx::query("SELECT version FROM _sqlx_migrations ORDER BY version")
        .fetch_all(db.owner_pool())
        .await
        .expect("read the shared migration ledger")
        .iter()
        .map(|row| row.get::<i64, _>("version"))
        .collect();
    assert!(
        versions
            .iter()
            .any(|v| (9_113_000_001..=9_113_999_999).contains(v)),
        "the engine's reserved range is absent from {versions:?}"
    );
    assert!(
        versions.contains(&1),
        "the directory set is absent from {versions:?}"
    );
    assert!(
        versions.iter().any(|v| *v > 20_260_000_000_000),
        "the sample service's timestamp set is absent from {versions:?}"
    );

    let assignment = Uuid::now_v7();
    let tenant = Uuid::now_v7();
    let principal = SamplePrincipal::new(Uuid::now_v7(), tenant);
    let mut tx = db
        .app_pool()
        .begin()
        .await
        .expect("open a write transaction");
    SampleRls
        .apply(&mut tx, &principal)
        .await
        .expect("the sample RLS applier prepares the connection");
    sqlx::query("INSERT INTO sample_assignment (id, tenant_id, title) VALUES ($1, $2, $3)")
        .bind(assignment)
        .bind(tenant)
        .bind("first")
        .execute(&mut *tx)
        .await
        .expect("the runtime role writes the service's own tables under its own tenant");
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM sample_assignment")
        .fetch_one(&mut *tx)
        .await
        .expect("read back under RLS");
    assert_eq!(visible, 1);
    tx.commit().await.expect("commit");

    let outsider = SamplePrincipal::new(Uuid::now_v7(), Uuid::now_v7());
    let mut tx = db
        .app_pool()
        .begin()
        .await
        .expect("open a read transaction");
    SampleRls
        .apply(&mut tx, &outsider)
        .await
        .expect("apply RLS");
    let leaked: i64 = sqlx::query_scalar("SELECT count(*) FROM sample_assignment")
        .fetch_one(&mut *tx)
        .await
        .expect("read back under another tenant");
    assert_eq!(leaked, 0, "the tenant policy is transaction-local and real");
    tx.rollback().await.expect("rollback");
    sqlx::query(
        "INSERT INTO service_engine.scheduled_impact (id, at, noun, key) \
         VALUES ($1, now(), 'assignment', $2)",
    )
    .bind(Uuid::now_v7())
    .bind(serde_json::json!(assignment))
    .execute(db.app_pool())
    .await
    .expect("the runtime role writes the engine's own tables");

    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    fabric
        .ping()
        .await
        .expect("the broker answers a round trip");

    let (coords, event) = sample_event();
    fabric
        .publish_event(&coords, &event)
        .await
        .expect("the fixed INTEGRATION_EVT stream binds and accepts the frame");

    PublishedLanguageReader::<Ping>::open(&fabric)
        .await
        .expect("the fixed PUBLISHED_LANGUAGE bucket binds");

    db.cleanup().await;
}

async fn table_exists(pool: &sqlx::PgPool, schema: &str, table: &str) -> bool {
    sqlx::query(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
    )
    .bind(schema)
    .bind(table)
    .fetch_optional(pool)
    .await
    .expect("read information_schema")
    .is_some()
}

#[tokio::test]
async fn brokers_spawned_at_once_never_share_a_port_or_a_jetstream() {
    let (one, two) = tokio::join!(TestNats::spawn(), TestNats::spawn());
    assert_ne!(one.url(), two.url(), "each broker owns its own port");

    one.provision().await;
    let (coords, event) = sample_event();

    one.fabric()
        .await
        .publish_event(&coords, &event)
        .await
        .expect("the provisioned broker accepts the frame");
    two.fabric()
        .await
        .publish_event(&coords, &event)
        .await
        .expect_err(
            "the second broker has no INTEGRATION_EVT stream, so it cannot be the first one",
        );
}

fn sample_event() -> (EventCoords, IntegrationEvent<Ping>) {
    let coords = EventCoords {
        producer: Bc::new("sample").unwrap(),
        aggregate: Aggregate::new("assignment").unwrap(),
        fact: PastFact::new("created").unwrap(),
        version: 1,
    };
    let event = IntegrationEvent::new(
        Uuid::now_v7(),
        "sample.assignment.created",
        1,
        Utc::now(),
        EventMetadata::new(Actor::Human(UserId::from(Uuid::now_v7())), Uuid::now_v7()),
        Ping {
            label: "smoke".into(),
        },
    );
    (coords, event)
}
