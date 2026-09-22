mod blackbox_support;

use std::time::Duration;

use blackbox_support::pg::BlackboxDb;
use conformance_service_engine::sample::library;
use service_engine::engine::boot::{apply_migration_chain, ensure_migrated};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

async fn ledger_max(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT max(version) FROM _sqlx_migrations")
        .fetch_one(pool)
        .await
        .expect("read the shared migration ledger")
}

async fn version_applied(pool: &PgPool, version: i64) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM _sqlx_migrations WHERE version = $1)")
        .bind(version)
        .fetch_one(pool)
        .await
        .expect("read the shared migration ledger")
}

#[tokio::test]
async fn s217_a_library_added_to_an_already_migrated_store_applies_below_the_max() {
    let db = BlackboxDb::fresh().await;
    let owner = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db.owner_url)
        .await
        .expect("connect as the owner");

    apply_migration_chain(
        &owner,
        vec![],
        library::base_service_migrator(),
        &db.app_role,
        Duration::from_secs(30),
    )
    .await
    .expect("a 0.2 adopter migrates engine and its service set with no library");
    let max_before = ledger_max(&owner).await;
    assert_eq!(
        max_before,
        library::BASE_SERVICE_VERSION,
        "the service set is the highest applied version before the library is adopted"
    );

    apply_migration_chain(
        &owner,
        vec![library::migrations()],
        library::dependent_service_migrator(),
        &db.app_role,
        Duration::from_secs(30),
    )
    .await
    .expect("adopting a library on the migrated store applies its band below the max");
    assert!(
        version_applied(&owner, library::ITEM_VERSION).await,
        "the library migration is applied although its version sits below max(applied)"
    );
    assert_eq!(
        ledger_max(&owner).await,
        max_before,
        "the library version did not become the new maximum"
    );

    ensure_migrated(
        &db.app,
        &[library::migrations()],
        &library::dependent_service_migrator(),
    )
    .await
    .expect("serve is ready once the library and dependent service sets are applied");

    let item = Uuid::now_v7();
    sqlx::query("INSERT INTO sample_lib.item (id, label) VALUES ($1, 'seed')")
        .bind(item)
        .execute(&db.app)
        .await
        .expect("the library table exists and is writable after adoption");
    sqlx::query("INSERT INTO sample_item_pin (id, item_id) VALUES ($1, $2)")
        .bind(Uuid::now_v7())
        .bind(item)
        .execute(&db.app)
        .await
        .expect("the dependent service table now references the library");

    apply_migration_chain(
        &owner,
        vec![],
        library::base_service_migrator(),
        &db.app_role,
        Duration::from_secs(30),
    )
    .await
    .expect("dropping the library again is tolerated by the shared ledger");
    ensure_migrated(&db.app, &[], &library::base_service_migrator())
        .await
        .expect("serve stays ready when the library is no longer declared");

    owner.close().await;
    db.cleanup().await;
}
