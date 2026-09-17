mod blackbox_support;

use std::time::Duration;

use blackbox_support::bin::{SpawnEnv, free_port, read_log, spawn_migrate_owner_only, wait_exit};
use blackbox_support::pg;
use conformance_service_engine::infra::TestNats;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
async fn bb14_migrate_needs_only_the_owner_env() {
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&pg::admin_url())
        .await
        .expect("connect to postgres as the superuser");

    let suffix = Uuid::now_v7().simple().to_string();
    let database = format!("bb14_{suffix}_db");
    let owner_role = format!("bb14_{suffix}_owner");
    let app_role = format!("bb14_{suffix}_app");
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
    run(
        &admin,
        &format!("GRANT CONNECT ON DATABASE \"{database}\" TO \"{app_role}\""),
    )
    .await;

    let nats = TestNats::spawn().await;
    let env = SpawnEnv {
        database_url: pg::role_url(&app_role, &database),
        owner_database_url: pg::role_url(&owner_role, &database),
        app_role: app_role.clone(),
        nats_url: nats.url(),
        pod: "bb14-pod".to_string(),
        port: free_port(),
        blobs: None,
        session_bounds: None,
        mirror: None,
    };

    let (mut child, log) = spawn_migrate_owner_only(&env, "bb14");

    let status = wait_exit(&mut child, Duration::from_secs(30))
        .await
        .expect("migrate finishes with only DATABASE_URL_OWNER and APP_ROLE in the environment");
    assert!(
        status.success(),
        "migrate builds its config from the owner-only env and exits 0, got {status}:\n{}",
        read_log(&log)
    );

    let app = PgPoolOptions::new()
        .max_connections(2)
        .connect(&pg::role_url(&app_role, &database))
        .await
        .expect("the app role can connect after the grant");
    sqlx::query("SELECT 1 FROM service_engine.leader_slot LIMIT 0")
        .execute(&app)
        .await
        .expect("the app role holds SELECT on the engine schema, so migrate granted it");
    app.close().await;

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
    drop(nats);
}

async fn run(pool: &PgPool, sql: &str) {
    sqlx::query(sql)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}
