mod blackbox_support;

use std::time::Duration;

use blackbox_support::pg::BlackboxDb;
use conformance_service_engine::sample::library;
use service_engine::engine::boot::apply_migration_chain;
use service_engine::error::EngineError;
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
async fn s215_a_library_migration_outside_its_declared_band_is_refused() {
    let db = BlackboxDb::fresh().await;
    let owner = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db.owner_url)
        .await
        .expect("connect as the owner");

    let mut lib = library::migrations();
    lib.band = (library::ITEM_VERSION + 1)..=*library::BAND.end();

    let migrate = apply_migration_chain(
        &owner,
        vec![lib],
        library::empty_service_migrator(),
        &db.app_role,
        Duration::from_secs(30),
    )
    .await;
    assert!(
        matches!(
            migrate,
            Err(EngineError::MigrationOutsideBand {
                owner: "sample_lib",
                version
            }) if version == library::ITEM_VERSION
        ),
        "a library migration below its band is refused naming the version, got {migrate:?}"
    );

    let engine_schema_present: bool =
        sqlx::query_scalar("SELECT to_regclass('service_engine.leader_slot') IS NOT NULL")
            .fetch_one(&owner)
            .await
            .expect("ask whether the engine schema was touched");
    assert!(
        !engine_schema_present,
        "the out-of-band migration is refused before any SQL runs"
    );

    owner.close().await;
    db.cleanup().await;
}
