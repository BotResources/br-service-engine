mod blackbox_support;

use std::time::Duration;

use blackbox_support::pg::BlackboxDb;
use conformance_service_engine::sample::library;
use service_engine::engine::boot::{
    REASON_MIGRATIONS_PENDING, apply_migration_chain, ensure_migrated,
};
use service_engine::error::EngineError;
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
async fn s216_serve_refuses_when_a_declared_library_set_is_unapplied_and_names_it() {
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
    .expect("engine and service sets apply with no library declared");

    let outcome = ensure_migrated(
        &db.app,
        &[library::migrations()],
        &library::base_service_migrator(),
    )
    .await;
    match outcome {
        Err(EngineError::MigrationsPending {
            engine,
            libraries,
            service,
        }) => {
            assert!(!engine, "the engine set is applied");
            assert!(!service, "the service set is applied");
            assert_eq!(
                libraries,
                vec![library::NAME],
                "the pending library is named"
            );
        }
        other => panic!("serve must refuse a store missing a declared library, got {other:?}"),
    }

    assert!(
        REASON_MIGRATIONS_PENDING.contains("library"),
        "the readiness reason names the library set"
    );

    owner.close().await;
    db.cleanup().await;
}
