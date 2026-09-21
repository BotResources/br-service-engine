mod blackbox_support;

use std::time::Duration;

use blackbox_support::pg::BlackboxDb;
use conformance_service_engine::sample::library;
use service_engine::engine::boot::apply_migration_chain;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
async fn s213_the_chain_applies_engine_then_library_then_service_and_grants_both_schemas() {
    let db = BlackboxDb::fresh().await;
    let owner = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db.owner_url)
        .await
        .expect("connect as the owner");

    apply_migration_chain(
        &owner,
        vec![library::migrations()],
        library::dependent_service_migrator(),
        &db.app_role,
        Duration::from_secs(30),
    )
    .await
    .expect("the migration chain applies engine, library and service in order");

    let item = Uuid::now_v7();
    sqlx::query("INSERT INTO sample_lib.item (id, label) VALUES ($1, 'seed')")
        .bind(item)
        .execute(&db.app)
        .await
        .expect("the app role writes the library table it was granted");

    sqlx::query("INSERT INTO sample_item_pin (id, item_id) VALUES ($1, $2)")
        .bind(Uuid::now_v7())
        .bind(item)
        .execute(&db.app)
        .await
        .expect("the app role writes the service table that references the library");

    let orphan = sqlx::query("INSERT INTO sample_item_pin (id, item_id) VALUES ($1, $2)")
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .execute(&db.app)
        .await;
    assert!(
        orphan.is_err(),
        "the cross-schema foreign key is enforced: a pin without a library item is refused"
    );

    owner.close().await;
    db.cleanup().await;
}
