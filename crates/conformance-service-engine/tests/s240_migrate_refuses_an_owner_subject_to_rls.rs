mod blackbox_support;

use std::time::Duration;

use blackbox_support::pg;
use conformance_service_engine::TestDb;
use conformance_service_engine::sample::library;
use service_engine::engine::boot::{apply_migration_chain, assert_owner_posture};
use service_engine::error::EngineError;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
async fn s240_the_owner_posture_passes_a_superuser_or_a_bypassrls_role_and_refuses_any_other() {
    let db = TestDb::fresh().await;

    let refusal = assert_owner_posture(db.owner_pool()).await;
    assert!(
        matches!(&refusal, Err(EngineError::OwnerSubjectToRls { role }) if role == db.owner_role()),
        "a CREATEROLE owner without BYPASSRLS is refused, naming the role: {refusal:?}"
    );
    let message = refusal.unwrap_err().to_string();
    assert!(
        message.contains("BYPASSRLS"),
        "the refusal names the missing attribute: {message}"
    );

    let bypass = db
        .pool_as(db.bypass_role())
        .await
        .expect("a pool on the BYPASSRLS role");
    assert_owner_posture(&bypass)
        .await
        .expect("a role carrying BYPASSRLS may run migrate");
    bypass.close().await;

    let superuser = db.superuser_pool().await;
    assert_owner_posture(&superuser)
        .await
        .expect("a superuser bypasses row-level security, so it may run migrate");
    superuser.close().await;

    db.cleanup().await;
}

#[tokio::test]
async fn s240_the_migration_chain_refuses_an_owner_subject_to_rls_before_it_applies_anything() {
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect(&pg::admin_url())
        .await
        .expect("connect to postgres as the superuser");

    let suffix = Uuid::now_v7().simple().to_string();
    let database = format!("s240_{suffix}_db");
    let owner_role = format!("s240_{suffix}_owner");
    let app_role = format!("s240_{suffix}_app");
    let password = pg::ROLE_PASSWORD;
    run(
        &admin,
        &format!(
            "CREATE ROLE \"{owner_role}\" LOGIN CREATEROLE NOSUPERUSER NOBYPASSRLS PASSWORD '{password}'"
        ),
    )
    .await;
    run(
        &admin,
        &format!("CREATE DATABASE \"{database}\" OWNER \"{owner_role}\""),
    )
    .await;
    run(
        &admin,
        &format!("CREATE ROLE \"{app_role}\" LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '{password}'"),
    )
    .await;

    let owner = PgPoolOptions::new()
        .max_connections(2)
        .connect(&pg::role_url(&owner_role, &database))
        .await
        .expect("connect as the owner");
    let refusal = apply_migration_chain(
        &owner,
        vec![library::migrations()],
        library::dependent_service_migrator(),
        &app_role,
        Duration::from_secs(5),
    )
    .await;
    assert!(
        matches!(&refusal, Err(EngineError::OwnerSubjectToRls { role }) if *role == owner_role),
        "the chain refuses an owner subject to RLS: {refusal:?}"
    );

    let touched: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname IN ('service_engine', 'sample_lib')) \
         OR to_regclass('public._sqlx_migrations') IS NOT NULL",
    )
    .fetch_one(&owner)
    .await
    .expect("inspect the database");
    assert!(
        !touched,
        "the refusal lands before the first migration: no engine or library schema, no ledger"
    );
    owner.close().await;

    run(
        &admin,
        &format!("DROP DATABASE IF EXISTS \"{database}\" WITH (FORCE)"),
    )
    .await;
    for role in [&app_role, &owner_role] {
        let _ = sqlx::query(&format!("DROP ROLE IF EXISTS \"{role}\""))
            .execute(&admin)
            .await;
    }
    admin.close().await;
}

async fn run(pool: &PgPool, sql: &str) {
    sqlx::query(sql)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}
