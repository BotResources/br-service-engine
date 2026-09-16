use sqlx::PgPool;
use sqlx::migrate::{MigrateError, Migrator};

/// The service's own migration set, tolerant of the engine migrations that share the
/// `_sqlx_migrations` ledger (`set_ignore_missing`). Handed to the boot kit, which runs
/// it under the owner role after the engine set.
pub fn migrator() -> Migrator {
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.set_ignore_missing(true);
    migrator
}

pub async fn migrate(pool: &PgPool) -> Result<(), MigrateError> {
    migrator().run(pool).await
}
