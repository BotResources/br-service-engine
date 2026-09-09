use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

pub const ROLE_PASSWORD: &str = "example_e2e_only";

pub fn admin_url() -> String {
    std::env::var("E2E_PG_ADMIN_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .expect("E2E_PG_ADMIN_URL or DATABASE_URL must point at a real PostgreSQL superuser")
}

fn url_for(admin: &str, role: &str, database: &str) -> String {
    let base = admin.split('@').next_back().unwrap_or("localhost:5432/postgres");
    let host_port = base.split('/').next().unwrap_or("localhost:5432");
    format!("postgresql://{role}:{ROLE_PASSWORD}@{host_port}/{database}")
}

pub struct TestDb {
    pub admin: PgPool,
    pub app: PgPool,
    pub database: String,
    pub owner_role: String,
    pub app_role: String,
}

impl TestDb {
    pub async fn fresh() -> Self {
        let admin_url = admin_url();
        let admin = pool(&admin_url).await;

        let suffix = Uuid::now_v7().simple().to_string();
        let database = format!("ex_{suffix}_db");
        let owner_role = format!("ex_{suffix}_owner");
        let app_role = format!("ex_{suffix}_app");

        run(
            &admin,
            &format!(
                "CREATE ROLE \"{owner_role}\" LOGIN CREATEROLE NOSUPERUSER NOBYPASSRLS PASSWORD '{ROLE_PASSWORD}'"
            ),
        )
        .await;
        run(
            &admin,
            &format!("CREATE DATABASE \"{database}\" OWNER \"{owner_role}\""),
        )
        .await;

        let owner = pool(&url_for(&admin_url, &owner_role, &database)).await;
        br_util_postgres::ensure_app_role(&owner, &app_role, ROLE_PASSWORD)
            .await
            .expect("provision the runtime app role");
        run(
            &admin,
            &format!("GRANT CONNECT ON DATABASE \"{database}\" TO \"{app_role}\""),
        )
        .await;

        service_engine::schema::migrate(&owner)
            .await
            .expect("apply the engine migration set");
        example_service::db::migrate(&owner)
            .await
            .expect("apply the example service migration set");

        br_util_postgres::grant_app_access(&owner, &app_role)
            .await
            .expect("grant the public schema to the app role");
        service_engine::schema::grant_engine_access(&owner, &app_role)
            .await
            .expect("grant the engine schema to the app role");

        let app = pool(&url_for(&admin_url, &app_role, &database)).await;
        owner.close().await;

        Self {
            admin,
            app,
            database,
            owner_role,
            app_role,
        }
    }

    pub async fn cleanup(self) {
        let admin = self.admin.clone();
        self.app.close().await;
        let _ = sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
            self.database
        ))
        .execute(&admin)
        .await;
        for role in [self.app_role, self.owner_role] {
            let _ = sqlx::query(&format!("DROP ROLE IF EXISTS \"{role}\""))
                .execute(&admin)
                .await;
        }
        admin.close().await;
    }
}

async fn pool(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(6)
        .connect(url)
        .await
        .unwrap_or_else(|e| panic!("connect to postgres: {e}"))
}

async fn run(pool: &PgPool, sql: &str) {
    sqlx::query(sql)
        .execute(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}
