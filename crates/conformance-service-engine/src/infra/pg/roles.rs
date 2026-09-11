use sqlx::PgPool;

use super::run;

pub async fn ensure_app_role(owner: &PgPool, role: &str, password: &str) {
    run(
        owner,
        &format!(
            "DO $$ BEGIN \
               IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN \
                 CREATE ROLE \"{role}\" LOGIN; \
               END IF; \
             END $$"
        ),
    )
    .await;
    run(
        owner,
        &format!("ALTER ROLE \"{role}\" LOGIN PASSWORD '{password}'"),
    )
    .await;
}

pub async fn grant_app_access(owner: &PgPool, role: &str) {
    for sql in [
        format!("GRANT USAGE ON SCHEMA public TO \"{role}\""),
        format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO \"{role}\""
        ),
        format!("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO \"{role}\""),
        format!(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public \
             GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"{role}\""
        ),
        format!(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO \"{role}\""
        ),
    ] {
        run(owner, &sql).await;
    }
}
