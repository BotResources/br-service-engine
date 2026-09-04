use std::str::FromStr;

use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

mod naming;

use naming::{describe, disposable_suffix, url_for};

use crate::infra::sweep::sweep_stale;
use crate::sample;

pub const ADMIN_URL_VAR: &str = "E2E_PG_ADMIN_URL";
pub const FALLBACK_URL_VAR: &str = "DATABASE_URL";
pub const ROLE_PASSWORD: &str = "engine_conformance_only";

const POOL_SIZE: u32 = 4;

pub fn admin_url() -> String {
    std::env::var(ADMIN_URL_VAR)
        .or_else(|_| std::env::var(FALLBACK_URL_VAR))
        .unwrap_or_else(|_| {
            panic!(
                "{ADMIN_URL_VAR} is not set: the conformance battery runs against a real \
                 PostgreSQL superuser and must never pass without one"
            )
        })
}

pub struct TestDb {
    admin: PgPool,
    owner: PgPool,
    app: PgPool,
    database: String,
    owner_role: String,
    app_role: String,
    bypass_role: String,
    member_role: String,
    admin_url: String,
    spares: std::sync::Mutex<Vec<(String, PgPool)>>,
}

impl TestDb {
    pub async fn fresh() -> Self {
        let admin_url = admin_url();
        let admin = pool(&admin_url).await;
        sweep_stale(&admin).await;

        let suffix = disposable_suffix();
        let database = format!("se_{suffix}_db");
        let owner_role = format!("se_{suffix}_owner");
        let app_role = format!("se_{suffix}_app");
        let bypass_role = format!("se_{suffix}_bypass");
        let member_role = format!("se_{suffix}_member");

        run(
            &admin,
            &format!(
                "CREATE ROLE \"{owner_role}\" LOGIN CREATEROLE NOSUPERUSER NOBYPASSRLS \
                 PASSWORD '{ROLE_PASSWORD}'"
            ),
        )
        .await;
        run(
            &admin,
            &format!(
                "CREATE ROLE \"{bypass_role}\" LOGIN NOSUPERUSER BYPASSRLS \
                 PASSWORD '{ROLE_PASSWORD}'"
            ),
        )
        .await;
        run(
            &admin,
            &format!(
                "CREATE ROLE \"{member_role}\" LOGIN NOINHERIT NOSUPERUSER NOBYPASSRLS \
                 PASSWORD '{ROLE_PASSWORD}'"
            ),
        )
        .await;
        run(
            &admin,
            &format!("GRANT \"{owner_role}\" TO \"{member_role}\""),
        )
        .await;
        run(
            &admin,
            &format!("CREATE DATABASE \"{database}\" OWNER \"{owner_role}\""),
        )
        .await;

        let owner = pool(&url_for(&admin_url, &owner_role, ROLE_PASSWORD, &database)).await;
        br_util_postgres::ensure_app_role(&owner, &app_role, ROLE_PASSWORD)
            .await
            .expect("provision the runtime app role");

        for role in [&app_role, &bypass_role, &member_role] {
            run(
                &admin,
                &format!("GRANT CONNECT ON DATABASE \"{database}\" TO \"{role}\""),
            )
            .await;
        }

        br_util_directory::migrate(&owner)
            .await
            .expect("apply the directory migration set");
        service_engine::schema::migrate(&owner)
            .await
            .expect("apply the engine migration set");
        sample::migrate(&owner)
            .await
            .expect("apply the sample service migration set");

        for role in [&app_role, &bypass_role] {
            br_util_postgres::grant_app_access(&owner, role)
                .await
                .expect("grant the public schema");
            service_engine::schema::grant_engine_access(&owner, role)
                .await
                .expect("grant the engine schema");
        }

        let app = pool(&url_for(&admin_url, &app_role, ROLE_PASSWORD, &database)).await;

        Self {
            admin,
            owner,
            app,
            database,
            owner_role,
            app_role,
            bypass_role,
            member_role,
            admin_url,
            spares: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn admin_pool(&self) -> &PgPool {
        &self.admin
    }

    pub fn owner_pool(&self) -> &PgPool {
        &self.owner
    }

    pub fn app_pool(&self) -> &PgPool {
        &self.app
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn owner_role(&self) -> &str {
        &self.owner_role
    }

    pub fn app_role(&self) -> &str {
        &self.app_role
    }

    pub fn bypass_role(&self) -> &str {
        &self.bypass_role
    }

    pub fn member_role(&self) -> &str {
        &self.member_role
    }

    pub fn url_as(&self, role: &str) -> String {
        url_for(&self.admin_url, role, ROLE_PASSWORD, &self.database)
    }

    pub async fn pool_as(&self, role: &str) -> Result<PgPool, sqlx::Error> {
        PgPoolOptions::new()
            .max_connections(2)
            .connect(&self.url_as(role))
            .await
    }

    pub async fn superuser_pool(&self) -> PgPool {
        let options = PgConnectOptions::from_str(&self.admin_url)
            .expect("the admin url parses")
            .database(&self.database);
        PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await
            .expect("a superuser pool on the fresh database")
    }

    pub async fn spare_database(&self, tag: &str) -> PgPool {
        let database = format!("{}_{tag}", self.database);
        run(
            &self.admin,
            &format!("DROP DATABASE IF EXISTS \"{database}\" WITH (FORCE)"),
        )
        .await;
        run(
            &self.admin,
            &format!(
                "CREATE DATABASE \"{database}\" OWNER \"{}\"",
                self.owner_role
            ),
        )
        .await;
        let pool = pool(&url_for(
            &self.admin_url,
            &self.owner_role,
            ROLE_PASSWORD,
            &database,
        ))
        .await;
        self.spares
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push((database, pool.clone()));
        pool
    }

    pub async fn terminate_backends(&self, application_name: &str) {
        let sql = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                   WHERE datname = $1 AND application_name = $2 AND pid <> pg_backend_pid()";
        let _ = sqlx::query(sql)
            .bind(&self.database)
            .bind(application_name)
            .execute(&self.admin)
            .await;
    }

    pub async fn cleanup(self) {
        let admin = self.admin.clone();
        let database = self.database.clone();
        let spares = std::mem::take(
            &mut *self
                .spares
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        for (name, pool) in spares {
            pool.close().await;
            let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
                .execute(&admin)
                .await;
        }
        let roles = [
            self.app_role,
            self.member_role,
            self.owner_role,
            self.bypass_role,
        ];
        self.owner.close().await;
        self.app.close().await;
        let _ = sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{database}\" WITH (FORCE)"
        ))
        .execute(&admin)
        .await;
        for role in roles {
            let _ = sqlx::query(&format!("DROP ROLE IF EXISTS \"{role}\""))
                .execute(&admin)
                .await;
        }
        admin.close().await;
    }
}

async fn pool(url: &str) -> PgPool {
    br_util_postgres::validate_database_tls(url).expect("the connection url carries a TLS posture");
    PgPoolOptions::new()
        .max_connections(POOL_SIZE)
        .connect(url)
        .await
        .unwrap_or_else(|e| panic!("connect to {}: {e}", describe(url)))
}

async fn run(pool: &PgPool, sql: &str) {
    sqlx::query(sql)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}
