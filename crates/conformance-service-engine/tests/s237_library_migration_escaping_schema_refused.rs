mod blackbox_support;

use std::time::Duration;

use blackbox_support::pg::BlackboxDb;
use conformance_service_engine::sample::library;
use service_engine::engine::boot::apply_migration_chain;
use service_engine::error::EngineError;
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
async fn s237_a_library_migration_that_creates_a_relation_outside_its_schema_fails_migrate() {
    let db = BlackboxDb::fresh().await;
    let owner = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db.owner_url)
        .await
        .expect("connect as the owner");

    let error = apply_migration_chain(
        &owner,
        vec![library::escaping_migrations()],
        library::empty_service_migrator(),
        &db.app_role,
        Duration::from_secs(30),
    )
    .await
    .expect_err("a library that touches public must fail migrate, not be granted silently");

    assert!(
        matches!(
            &error,
            EngineError::LibraryMigrationEscapedSchema { library, object }
                if *library == "sample_lib" && object == "public.sample_lib_escapee"
        ),
        "the failure names the library and the escaped relation: {error:?}",
    );

    owner.close().await;
    db.cleanup().await;
}
