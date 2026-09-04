use std::sync::Arc;
use std::time::Duration;

use br_util_nats_fabric::{DEFAULT_MAX_MESSAGES, OutboxRelay};
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::{delivered_event_ids, stage_outbox_row};
use service_engine::housekeeping::relay::RelayRuntime;
use service_engine::name::{PodId, RelayName};
use service_engine::relays::outbox::FabricOutboxRelay;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const OUTBOX: RelayName = RelayName::from_static("integration_outbox");

#[tokio::test]
async fn s15_a_hosted_relay_drains_on_a_pool_the_runtime_never_holds() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;

    let mut tx = db.app_pool().begin().await.expect("the write transaction");
    let staged = stage_outbox_row(&mut tx, "hosted-standalone").await;
    tx.commit().await.expect("commit");

    let single = single_connection_pool(&db).await;
    let relay = Arc::new(FabricOutboxRelay::hosting(
        OUTBOX,
        OutboxRelay::new(single.clone(), fabric.clone()),
        DEFAULT_MAX_MESSAGES,
    ));
    let mut engine = RelayRuntime::new(PodId::new("se-relay-0").expect("a valid pod id"));
    engine
        .register_erased(relay.clone())
        .expect("the hosted relay registers");

    let round = engine.after_pass(&single).await;

    assert_eq!(
        round.failed, 0,
        "the runtime must not open a transaction of its own for a relay that opens its own: on a \
         pool of one connection the hosted relay would wait for the connection the runtime is \
         holding, and the drain would time out acquiring it"
    );
    assert_eq!(round.ran, 1);
    assert_eq!(round.rows, 1, "the staged row left on the first pass");
    assert!(engine.health().borrow().is_healthy());
    assert_eq!(
        delivered_event_ids(&fabric, "se-observer-hosted").await,
        vec![staged]
    );
    assert_eq!(
        idle_in_transaction(db.app_pool()).await,
        0,
        "a standalone drain leaves no engine transaction open behind it"
    );

    single.close().await;
    db.cleanup().await;
}

async fn single_connection_pool(db: &TestDb) -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(3))
        .connect(&db.url_as(db.app_role()))
        .await
        .expect("a pool holding exactly one connection")
}

async fn idle_in_transaction(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity \
          WHERE datname = current_database() AND state = 'idle in transaction'",
    )
    .fetch_one(pool)
    .await
    .expect("read the backends of this database")
}
