//! s241 — an engine migration whose bytes changed after a release applied it.
//!
//! sqlx stores a SHA-384 of every applied file and refuses a ledger whose
//! checksum differs from the embedded file. The engine registers the checksum
//! of every released file it later edited, and `migrate` adopts exactly those:
//! a store a released engine migrated upgrades, an unknown checksum still fails
//! the way it always did, and a fresh store is untouched by the adoption.

mod blackbox_support;

use std::time::Duration;

use blackbox_support::pg::BlackboxDb;
use chrono::{DateTime, Utc};
use conformance_service_engine::sample::library;
use service_engine::engine::boot::apply_migration_chain;
use service_engine::error::EngineError;
use service_engine::schema::{RESERVED_VERSION_MAX, RESERVED_VERSION_MIN};
use sqlx::PgPool;
use sqlx::migrate::{MigrateError, Migrator};
use sqlx::postgres::PgPoolOptions;

/// The engine migration v0.3.0 edited (a header comment removed) after v0.2.0
/// had applied it in real databases.
const EDITED: i64 = 9_113_000_023;

/// The engine set byte for byte as the `v0.2.0` tag shipped it.
fn engine_v0_2_0() -> Migrator {
    let mut migrator = sqlx::migrate!("./migrations-engine-v0.2.0");
    migrator.set_ignore_missing(true);
    migrator
}

#[tokio::test]
async fn s241_a_store_engine_0_2_0_migrated_upgrades_and_its_ledger_takes_the_current_checksums() {
    let db = BlackboxDb::fresh().await;
    let owner = connect_owner(&db).await;
    migrate_as_engine_0_2_0(&owner).await;
    let released = engine_checksum(&owner, EDITED).await;
    let adopters_before = adopter_ledger(&owner).await;

    migrate(&owner, &db)
        .await
        .expect("migrate upgrades a store the v0.2.0 engine migrated");

    let reference = BlackboxDb::fresh().await;
    let reference_owner = connect_owner(&reference).await;
    migrate(&reference_owner, &reference)
        .await
        .expect("the current chain applies on a fresh store");
    assert_eq!(
        engine_ledger(&owner).await,
        engine_ledger(&reference_owner).await,
        "the upgraded store's engine ledger matches a fresh store's, checksum for checksum"
    );
    assert_ne!(
        engine_checksum(&owner, EDITED).await,
        released,
        "the released checksum was replaced by the current one"
    );
    assert_eq!(
        adopter_ledger(&owner).await,
        adopters_before,
        "library and service rows on the shared ledger are untouched by the adoption"
    );

    let settled = full_ledger(&owner).await;
    migrate(&owner, &db)
        .await
        .expect("a second migrate on the upgraded store succeeds");
    assert_eq!(
        full_ledger(&owner).await,
        settled,
        "a second migrate changes nothing on the ledger"
    );

    reference_owner.close().await;
    reference.cleanup().await;
    owner.close().await;
    db.cleanup().await;
}

#[tokio::test]
async fn s241_an_unknown_checksum_on_an_engine_migration_still_fails_the_migrate() {
    let db = BlackboxDb::fresh().await;
    let owner = connect_owner(&db).await;
    migrate(&owner, &db)
        .await
        .expect("the current chain applies on a fresh store");
    sqlx::query(
        "UPDATE _sqlx_migrations SET checksum = sha384('edited by hand'::bytea) WHERE version = $1",
    )
    .bind(EDITED)
    .execute(&owner)
    .await
    .expect("forge a checksum no engine release ever shipped");
    let forged = engine_checksum(&owner, EDITED).await;

    let refused = migrate(&owner, &db).await;

    assert!(
        matches!(
            refused,
            Err(EngineError::Migrate(MigrateError::VersionMismatch(EDITED)))
        ),
        "an unknown checksum fails with sqlx's version mismatch, exactly as before: {refused:?}"
    );
    assert_eq!(
        engine_checksum(&owner, EDITED).await,
        forged,
        "an unknown checksum is never rewritten"
    );

    owner.close().await;
    db.cleanup().await;
}

#[tokio::test]
async fn s241_a_fresh_store_migrates_as_before_and_a_second_run_changes_nothing() {
    let db = BlackboxDb::fresh().await;
    let owner = connect_owner(&db).await;

    migrate(&owner, &db)
        .await
        .expect("the current chain applies on a fresh store");
    let first = full_ledger(&owner).await;
    assert_eq!(
        engine_ledger(&owner).await.len(),
        25,
        "every engine migration is on the ledger"
    );

    migrate(&owner, &db)
        .await
        .expect("a second migrate on a current store succeeds");
    assert_eq!(
        full_ledger(&owner).await,
        first,
        "nothing on a current ledger is rewritten"
    );

    owner.close().await;
    db.cleanup().await;
}

async fn connect_owner(db: &BlackboxDb) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(&db.owner_url)
        .await
        .expect("connect as the owner")
}

async fn migrate(owner: &PgPool, db: &BlackboxDb) -> Result<(), EngineError> {
    apply_migration_chain(
        owner,
        vec![library::migrations()],
        library::dependent_service_migrator(),
        &db.app_role,
        Duration::from_secs(30),
    )
    .await
}

/// The ledger as an adopter on engine 0.2.0 left it: the engine set of that
/// release, then a library set and a service set on the same ledger.
async fn migrate_as_engine_0_2_0(owner: &PgPool) {
    engine_v0_2_0()
        .run(owner)
        .await
        .expect("the v0.2.0 engine set applies on a fresh store");
    let mut library = library::migrations().migrator;
    library.set_ignore_missing(true);
    library
        .run(owner)
        .await
        .expect("a library set applies after the v0.2.0 engine set");
    let mut service = library::dependent_service_migrator();
    service.set_ignore_missing(true);
    service
        .run(owner)
        .await
        .expect("a service set applies after the v0.2.0 engine set");
}

async fn engine_checksum(pool: &PgPool, version: i64) -> Vec<u8> {
    sqlx::query_scalar("SELECT checksum FROM _sqlx_migrations WHERE version = $1")
        .bind(version)
        .fetch_one(pool)
        .await
        .expect("read one engine row of the ledger")
}

async fn engine_ledger(pool: &PgPool) -> Vec<(i64, Vec<u8>)> {
    sqlx::query_as(
        "SELECT version, checksum FROM _sqlx_migrations \
         WHERE version BETWEEN $1 AND $2 ORDER BY version",
    )
    .bind(RESERVED_VERSION_MIN)
    .bind(RESERVED_VERSION_MAX)
    .fetch_all(pool)
    .await
    .expect("read the engine rows of the ledger")
}

async fn adopter_ledger(pool: &PgPool) -> Vec<(i64, Vec<u8>, DateTime<Utc>)> {
    sqlx::query_as(
        "SELECT version, checksum, installed_on FROM _sqlx_migrations \
         WHERE version NOT BETWEEN $1 AND $2 ORDER BY version",
    )
    .bind(RESERVED_VERSION_MIN)
    .bind(RESERVED_VERSION_MAX)
    .fetch_all(pool)
    .await
    .expect("read the library and service rows of the ledger")
}

async fn full_ledger(pool: &PgPool) -> Vec<(i64, Vec<u8>, DateTime<Utc>, bool)> {
    sqlx::query_as(
        "SELECT version, checksum, installed_on, success FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(pool)
    .await
    .expect("read the whole ledger")
}
