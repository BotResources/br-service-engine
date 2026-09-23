use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

pub const ROLE_PASSWORD: &str = "example_blackbox_only";

pub fn admin_url() -> String {
    std::env::var("E2E_PG_ADMIN_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .expect("E2E_PG_ADMIN_URL or DATABASE_URL must point at a real PostgreSQL superuser")
}

fn host_port(admin: &str) -> String {
    let base = admin
        .split('@')
        .next_back()
        .unwrap_or("localhost:5432/postgres");
    base.split('/')
        .next()
        .unwrap_or("localhost:5432")
        .to_string()
}

fn url_for(admin: &str, role: &str, database: &str) -> String {
    format!(
        "postgresql://{role}:{ROLE_PASSWORD}@{}/{database}",
        host_port(admin)
    )
}

pub fn role_url(role: &str, database: &str) -> String {
    url_for(&admin_url(), role, database)
}

pub struct BlackboxDb {
    pub admin: PgPool,
    pub app: PgPool,
    pub app_url: String,
    pub owner_url: String,
    pub app_role: String,
    database: String,
    owner_role: String,
}

impl BlackboxDb {
    pub async fn fresh() -> Self {
        let admin_url = admin_url();
        let admin = pool(&admin_url).await;

        let suffix = Uuid::now_v7().simple().to_string();
        let database = format!("bb_{suffix}_db");
        let owner_role = format!("bb_{suffix}_owner");
        let app_role = format!("bb_{suffix}_app");

        run(
            &admin,
            &format!(
                "CREATE ROLE \"{owner_role}\" LOGIN CREATEROLE NOSUPERUSER BYPASSRLS PASSWORD '{ROLE_PASSWORD}'"
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
            &format!(
                "CREATE ROLE \"{app_role}\" LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '{ROLE_PASSWORD}'"
            ),
        )
        .await;
        run(
            &admin,
            &format!("GRANT CONNECT ON DATABASE \"{database}\" TO \"{app_role}\""),
        )
        .await;

        let owner_url = url_for(&admin_url, &owner_role, &database);
        let app_url = url_for(&admin_url, &app_role, &database);
        let app = pool(&app_url).await;

        Self {
            admin,
            app,
            app_url,
            owner_url,
            app_role,
            database,
            owner_role,
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
        .max_connections(4)
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
