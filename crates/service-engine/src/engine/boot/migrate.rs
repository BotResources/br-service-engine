use std::time::{Duration, Instant};

use br_util_observability::init_logging;
use br_util_postgres::grant_app_access;
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;

use crate::engine::boot::env::owner_database_url;
use crate::engine::boot::libraries::{self, LibraryMigrations};
use crate::engine::boot::{BootPlan, pg_error};
use crate::error::EngineError;

const RETRY_CEILING: Duration = Duration::from_secs(5);
const RETRY_FLOOR: Duration = Duration::from_millis(250);

pub(crate) async fn migrate<Q, M, S, R>(plan: BootPlan<Q, M, S, R>) -> Result<(), EngineError> {
    init_logging(plan.component);

    let owner_url = owner_database_url()?;
    let owner = connect_owner(&owner_url, plan.config.migrate_connect_timeout).await?;

    apply_migration_chain(
        &owner,
        plan.libraries,
        plan.service_migrator,
        &plan.config.app_role,
        plan.config.migrate_connect_timeout,
    )
    .await?;

    owner.close().await;
    Ok(())
}

pub async fn apply_migration_chain(
    owner: &PgPool,
    libraries: Vec<LibraryMigrations>,
    service_migrator: Migrator,
    app_role: &str,
    role_timeout: Duration,
) -> Result<(), EngineError> {
    libraries::validate(&libraries, &service_migrator)?;

    crate::schema::migrate(owner).await?;

    let schemas: Vec<&'static str> = libraries.iter().map(|library| library.schema).collect();
    for library in libraries {
        let mut migrator = library.migrator;
        migrator.set_ignore_missing(true);
        migrator.run(owner).await?;
    }

    let mut service_migrator = service_migrator;
    service_migrator.set_ignore_missing(true);
    service_migrator.run(owner).await?;

    let mut conn = owner.acquire().await.map_err(EngineError::Db)?;
    crate::relays::outbox::adopt_legacy_outbox(&mut conn).await?;
    drop(conn);

    wait_for_role(owner, app_role, role_timeout).await?;
    crate::schema::grant_schema_access(owner, crate::schema::SCHEMA, app_role).await?;
    for schema in schemas {
        crate::schema::grant_schema_access(owner, schema, app_role).await?;
    }
    grant_app_access(owner, app_role).await.map_err(pg_error)?;
    Ok(())
}

async fn connect_owner(url: &str, timeout: Duration) -> Result<PgPool, EngineError> {
    crate::db::validate_database_tls(url)?;
    let deadline = Instant::now() + timeout;
    let mut backoff = RETRY_FLOOR;
    loop {
        match PgPoolOptions::new().max_connections(2).connect(url).await {
            Ok(pool) => return Ok(pool),
            Err(error) => {
                if Instant::now() >= deadline {
                    return Err(EngineError::Db(error));
                }
                tracing::warn!(%error, "migrate: owner connection not ready, retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RETRY_CEILING);
            }
        }
    }
}

async fn wait_for_role(pool: &PgPool, role: &str, timeout: Duration) -> Result<(), EngineError> {
    let deadline = Instant::now() + timeout;
    let mut backoff = RETRY_FLOOR;
    loop {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)")
                .bind(role)
                .fetch_one(pool)
                .await?;
        if exists {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(EngineError::Config(format!(
                "the app role {role} still does not exist after waiting {timeout:?}; gitops \
                 declares the role, migrate only grants it"
            )));
        }
        tracing::warn!(role, "migrate: waiting for the app role to exist");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(RETRY_CEILING);
    }
}
