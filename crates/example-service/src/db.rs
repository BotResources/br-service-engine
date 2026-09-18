use sqlx::PgPool;
use sqlx::migrate::{MigrateError, Migrator};

pub fn migrator() -> Migrator {
    sqlx::migrate!("./migrations")
}

pub async fn migrate(pool: &PgPool) -> Result<(), MigrateError> {
    let mut migrator = migrator();
    migrator.set_ignore_missing(true);
    migrator.run(pool).await
}
