mod blackbox_support;

use std::time::Duration;

use blackbox_support::bin::{SpawnEnv, free_port, read_log, spawn_migrate_owner_only, wait_exit};
use blackbox_support::pg;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
async fn bb15_migrate_refuses_an_owner_subject_to_rls_and_exits_non_zero() {
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&pg::admin_url())
        .await
        .expect("connect to postgres as the superuser");

    let suffix = Uuid::now_v7().simple().to_string();
    let database = format!("bb15_{suffix}_db");
    let owner_role = format!("bb15_{suffix}_owner");
    let app_role = format!("bb15_{suffix}_app");
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

    let env = SpawnEnv {
        database_url: pg::role_url(&app_role, &database),
        owner_database_url: pg::role_url(&owner_role, &database),
        app_role: app_role.clone(),
        nats_url: "nats://127.0.0.1:1".to_string(),
        pod: format!("bb15-{suffix}"),
        port: free_port(),
        blobs: None,
        session_bounds: None,
        mirror: None,
    };

    let (mut child, log) = spawn_migrate_owner_only(&env, "bb15");
    let status = wait_exit(&mut child, Duration::from_secs(30))
        .await
        .expect("migrate exits promptly instead of waiting on anything");
    let output = read_log(&log);
    assert!(
        !status.success(),
        "migrate refuses an owner without BYPASSRLS and exits non-zero, got {status}:\n{output}"
    );
    assert!(
        output.contains("BYPASSRLS") && output.contains(&owner_role),
        "the refusal names the owner role and the missing attribute:\n{output}"
    );

    let owner = PgPoolOptions::new()
        .max_connections(1)
        .connect(&pg::role_url(&owner_role, &database))
        .await
        .expect("connect as the owner to inspect the database");
    let migrated: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'service_engine')",
    )
    .fetch_one(&owner)
    .await
    .expect("inspect the database");
    assert!(
        !migrated,
        "the refusal lands before any migration is applied"
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
