use std::time::{Duration, Instant};

use br_util_observability::init_logging;
use br_util_postgres::grant_app_access;
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;

use crate::engine::boot::env::owner_database_url;
use crate::engine::boot::libraries::{self, LibraryMigrations};
use crate::engine::boot::owner::assert_owner_posture;
use crate::engine::boot::{BootPlan, pg_error};
use crate::error::EngineError;

const RETRY_CEILING: Duration = Duration::from_secs(5);
const RETRY_FLOOR: Duration = Duration::from_millis(250);

pub(crate) async fn migrate<Q, M, S, R>(plan: BootPlan<Q, M, S, R>) -> Result<(), EngineError> {
    init_logging(plan.component);

    let owner_url = owner_database_url()?;
    let owner = connect_owner(&owner_url, plan.config.migrate_connect_timeout).await?;

    let applied = apply_migration_chain(
        &owner,
        plan.libraries,
        plan.service_migrator,
        &plan.config.app_role,
        plan.config.migrate_connect_timeout,
    )
    .await;
    owner.close().await;
    if let Err(error) = &applied {
        tracing::error!(error = %crate::chain::describe(&error), "migrate failed");
    }
    applied
}

pub async fn apply_migration_chain(
    owner: &PgPool,
    libraries: Vec<LibraryMigrations>,
    service_migrator: Migrator,
    app_role: &str,
    role_timeout: Duration,
) -> Result<(), EngineError> {
    libraries::validate(&libraries, &service_migrator)?;
    assert_owner_posture(owner).await?;

    crate::schema::migrate(owner).await?;

    let schemas: Vec<&'static str> = libraries.iter().map(|library| library.schema).collect();
    for library in libraries {
        let name = library.name;
        let schema = library.schema;
        let before = relation_snapshot(owner).await?;
        let mut migrator = library.migrator;
        migrator.set_ignore_missing(true);
        migrator.run(owner).await?;
        let after = relation_snapshot(owner).await?;
        if let Some((nsp, rel)) = after
            .difference(&before)
            .find(|(nsp, _)| nsp.as_str() != schema)
        {
            return Err(EngineError::LibraryMigrationEscapedSchema {
                library: name,
                object: format!("{nsp}.{rel}"),
            });
        }
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

async fn relation_snapshot(
    pool: &PgPool,
) -> Result<std::collections::BTreeSet<(String, String)>, EngineError> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT n.nspname, c.relname \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') \
           AND n.nspname NOT LIKE 'pg\\_%' \
           AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f', 'c')",
    )
    .fetch_all(pool)
    .await
    .map_err(EngineError::Db)?;
    Ok(rows.into_iter().collect())
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
                tracing::warn!(error = %crate::chain::describe(&error), "migrate: owner connection not ready, retrying");
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
