//! The boot kit: one call from a service's `main` that stands the whole edge up.
//!
//! A service builds its [`EngineConfig`], connects NATS, and hands the engine its
//! GraphQL roots, its own migration set, and a registration closure. The kit then,
//! in order:
//!
//! 1. installs structured JSON logging (a tracing subscriber),
//! 2. short-circuits the `schema` subcommand — printing the composed SDL and exiting
//!    without touching Postgres or NATS,
//! 3. runs the engine and the service migration sets under the **owner** role and
//!    grants the app role, then drops the owner pool,
//! 4. connects the RLS-subject **app** pool the engine serves under,
//! 5. installs the process-global Prometheus recorder — against which the engine's
//!    existing `metrics!` set is already emitted, so `/metrics` exports it,
//! 6. boots the engine, registers the service's slices, builds the schema, mounts
//!    `/livez` + `/metrics` + `/sdl` beside the engine's own routes, and serves until
//!    shutdown.
//!
//! The owner/app split is not optional: the engine refuses to run under a role that
//! owns its schema (see [`crate::boot::assert_posture`]), so migrations must run under
//! a distinct owner role before the app pool ever connects. `DATABASE_URL_OWNER` names
//! the owner role; `DATABASE_URL` names the app role. Role and database provisioning
//! stay in GitOps — the kit only migrates, grants, and serves.

use async_graphql::{ObjectType, SubscriptionType};
use br_util_observability::{init_logging, init_metrics};
use br_util_postgres::{PostgresError, grant_app_access, init_migration_pool, init_pool};
use sqlx::migrate::Migrator;

use crate::config::EngineConfig;
use crate::engine::Engine;
use crate::error::EngineError;
use crate::graphql::PassportPrincipal;
use crate::nats::Nats;
use crate::readiness::ReadinessHandle;

/// The environment variable naming the app pool the engine serves under.
pub const DATABASE_URL: &str = "DATABASE_URL";

/// The CLI subcommand that prints the composed SDL and exits.
pub const SCHEMA_SUBCOMMAND: &str = "schema";

/// Everything a service hands the [`run_service`] boot kit.
///
/// `Q`/`M`/`S` are the composed GraphQL roots (`compose_service!` derives them);
/// `R` is the registration closure that binds the service's slices onto the engine.
/// The principal `P` is inferred from that closure.
pub struct BootPlan<Q, M, S, R> {
    /// Process identity for logs and the `component` Prometheus label.
    pub component: &'static str,
    /// The low-privilege runtime role the app pool connects as; grants target it.
    pub app_role: String,
    /// The service's own migration set, applied under the owner role after the engine's.
    pub service_migrator: Migrator,
    /// The fully-built engine configuration (channel, pod, service, http addr, timings).
    pub config: EngineConfig,
    /// The NATS URL to connect. Connected only on the serve path, never for `schema`.
    pub nats_url: String,
    /// The composed query root.
    pub query: Q,
    /// The composed mutation root.
    pub mutation: M,
    /// The composed subscription root.
    pub subscription: S,
    /// Whether to run the Identity scope-declaration handshake at boot.
    pub declare_scopes: bool,
    /// Binds the service's principal resolver, RLS, and slices onto the engine.
    pub register: R,
}

/// Stand the service up from `main` and serve until shutdown.
///
/// Returns `Ok(())` after a clean shutdown, or the first boot error. When invoked as
/// `<binary> schema` it prints the SDL and returns without connecting to any infra.
pub async fn run_service<P, Q, M, S, R>(plan: BootPlan<Q, M, S, R>) -> Result<(), EngineError>
where
    P: PassportPrincipal,
    Q: ObjectType + 'static,
    M: ObjectType + 'static,
    S: SubscriptionType + 'static,
    R: FnOnce(&mut Engine<P>) -> Result<(), EngineError>,
{
    let BootPlan {
        component,
        app_role,
        service_migrator,
        config,
        nats_url,
        query,
        mutation,
        subscription,
        declare_scopes,
        register,
    } = plan;

    if wants_schema_dump() {
        let schema = async_graphql::Schema::build(query, mutation, subscription).finish();
        println!("{}", schema.sdl());
        return Ok(());
    }

    init_logging(component);

    prepare_database(service_migrator, &app_role).await?;
    let pool = init_pool(&app_database_url()?).await.map_err(pg_error)?;
    let nats = Nats::connect(&nats_url).await?;

    // Install the process-global recorder before the engine emits its first metric, so
    // the engine's existing `service_engine_*` set lands in the same registry `/metrics`
    // renders. A double install (e.g. two engines in one process) is a boot misconfig.
    let metrics = init_metrics(component)
        .map_err(|e| EngineError::Config(format!("metrics recorder install failed: {e}")))?;

    let http_addr = config.http_addr;
    let readiness = ReadinessHandle::not_ready("booting");
    let mut engine = Engine::<P>::boot(config, pool, nats, readiness.clone()).await?;
    register(&mut engine)?;
    if declare_scopes {
        engine.declare_contributed_scopes()?;
    }

    let state = std::sync::Arc::new(engine.graphql_state());
    let schema = crate::graphql::engine_schema(query, mutation, subscription, state.clone());
    let sdl = schema.sdl();
    engine.set_schema_sdl(sdl.clone());

    let app = crate::graphql::with_edge_observability(
        crate::graphql::app(schema, state, readiness),
        sdl,
        metrics,
    );

    tracing::info!(component, %http_addr, "boot kit is serving");
    engine.run_with(app).await
}

async fn prepare_database(service_migrator: Migrator, app_role: &str) -> Result<(), EngineError> {
    let owner = init_migration_pool().await.map_err(pg_error)?;
    crate::schema::migrate(&owner).await?;
    service_migrator.run(&owner).await?;
    let mut conn = owner.acquire().await.map_err(EngineError::Db)?;
    crate::relays::outbox::adopt_legacy_outbox(&mut conn).await?;
    drop(conn);
    crate::schema::grant_engine_access(&owner, app_role).await?;
    grant_app_access(&owner, app_role).await.map_err(pg_error)?;
    owner.close().await;
    Ok(())
}

fn app_database_url() -> Result<String, EngineError> {
    std::env::var(DATABASE_URL)
        .map_err(|_| EngineError::Config(format!("{DATABASE_URL} must be set for the app pool")))
}

fn wants_schema_dump() -> bool {
    std::env::args().nth(1).as_deref() == Some(SCHEMA_SUBCOMMAND)
}

fn pg_error(error: PostgresError) -> EngineError {
    match error {
        PostgresError::Config(message) => EngineError::Config(message),
        PostgresError::InvalidRoleName(name) => EngineError::InvalidRoleName(name),
        PostgresError::Db(source) => EngineError::Db(source),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_postgres_error_maps_onto_the_matching_engine_error_variant() {
        assert!(matches!(
            pg_error(PostgresError::Config("bad url".into())),
            EngineError::Config(message) if message == "bad url"
        ));
        assert!(matches!(
            pg_error(PostgresError::InvalidRoleName("Bad".into())),
            EngineError::InvalidRoleName(name) if name == "Bad"
        ));
        assert!(matches!(
            pg_error(PostgresError::Db(sqlx::Error::PoolClosed)),
            EngineError::Db(sqlx::Error::PoolClosed)
        ));
    }
}
