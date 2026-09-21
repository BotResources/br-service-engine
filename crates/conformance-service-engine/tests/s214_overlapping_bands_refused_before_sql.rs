mod blackbox_support;

use std::time::Duration;

use blackbox_support::pg::BlackboxDb;
use conformance_service_engine::sample::library;
use service_engine::engine::boot::{apply_migration_chain, ensure_migrated};
use service_engine::error::EngineError;
use sqlx::postgres::PgPoolOptions;

fn overlapping() -> Vec<service_engine::LibraryMigrations> {
    let first = library::migrations();
    let mut second = library::migrations();
    second.name = "sample_lib_two";
    second.schema = "sample_lib_two";
    second.band = 9_120_500_000..=9_121_000_000;
    vec![first, second]
}

#[tokio::test]
async fn s214_overlapping_library_bands_fail_migrate_and_serve_before_touching_the_store() {
    let db = BlackboxDb::fresh().await;
    let owner = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db.owner_url)
        .await
        .expect("connect as the owner");

    let migrate = apply_migration_chain(
        &owner,
        overlapping(),
        library::empty_service_migrator(),
        &db.app_role,
        Duration::from_secs(30),
    )
    .await;
    assert!(
        matches!(
            migrate,
            Err(EngineError::MigrationBandOverlap {
                first: "sample_lib",
                second: "sample_lib_two"
            })
        ),
        "migrate refuses overlapping bands naming both, got {migrate:?}"
    );

    let engine_schema_present: bool =
        sqlx::query_scalar("SELECT to_regclass('service_engine.leader_slot') IS NOT NULL")
            .fetch_one(&owner)
            .await
            .expect("ask whether the engine schema was touched");
    assert!(
        !engine_schema_present,
        "validation runs before any SQL: the engine schema was never created"
    );

    let serve = ensure_migrated(&owner, &overlapping(), &library::empty_service_migrator()).await;
    assert!(
        matches!(serve, Err(EngineError::MigrationBandOverlap { .. })),
        "serve refuses the same overlapping declaration, got {serve:?}"
    );

    owner.close().await;
    db.cleanup().await;
}
