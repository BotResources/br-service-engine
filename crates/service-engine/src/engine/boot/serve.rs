use std::sync::Arc;

use async_graphql::{ObjectType, SubscriptionType};
use br_util_observability::{init_logging, init_metrics};
use br_util_postgres::{init_pool, migrations_status};
use sqlx::PgPool;
use sqlx::migrate::Migrator;

use crate::engine::Engine;
use crate::engine::boot::env::app_database_url;
use crate::engine::boot::libraries::LibraryMigrations;
use crate::engine::boot::{BootPlan, REASON_MIGRATIONS_PENDING, pg_error};
use crate::error::EngineError;
use crate::graphql::PassportPrincipal;
use crate::inbound::{REASON_MESSAGE_RETENTION, derive_message_retention};
use crate::nats::Nats;
use crate::readiness::ReadinessHandle;

pub(crate) async fn serve<P, Q, M, S, R>(plan: BootPlan<Q, M, S, R>) -> Result<(), EngineError>
where
    P: PassportPrincipal,
    Q: ObjectType + 'static,
    M: ObjectType + 'static,
    S: SubscriptionType + 'static,
    R: FnOnce(&mut Engine<P>) -> Result<(), EngineError>,
{
    let BootPlan {
        component,
        libraries,
        service_migrator,
        config,
        query,
        mutation,
        subscription,
        declare_scopes,
        register,
    } = plan;

    init_logging(component);

    let readiness = ReadinessHandle::not_ready("booting");
    let pool = init_pool(&app_database_url()?).await.map_err(pg_error)?;
    if let Err(error) = ensure_migrated(&pool, &libraries, &service_migrator).await {
        if matches!(error, EngineError::MigrationsPending { .. }) {
            readiness.set_not_ready(REASON_MIGRATIONS_PENDING);
            tracing::error!(%error, "refusing to serve a store that migrate has not finished");
        }
        return Err(error);
    }

    let nats = Nats::connect(&config.nats_url).await?;
    let metrics = init_metrics(component)
        .map_err(|e| EngineError::Config(format!("metrics recorder install failed: {e}")))?;
    let http_addr = config.http_addr;

    let mut engine = Engine::<P>::boot(config, pool, nats, readiness.clone()).await?;
    register(&mut engine)?;
    derive_retention(&mut engine, &readiness).await?;
    if declare_scopes {
        engine.declare_contributed_scopes()?;
    }

    let state = Arc::new(engine.graphql_state());
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

pub async fn ensure_migrated(
    pool: &PgPool,
    libraries: &[LibraryMigrations],
    service_migrator: &Migrator,
) -> Result<(), EngineError> {
    let engine_pending = !migrations_status(pool, &crate::schema::migrator())
        .await
        .map_err(pg_error)?
        .embedded_applied();
    let mut pending_libraries = Vec::new();
    for library in libraries {
        let applied = migrations_status(pool, &library.migrator)
            .await
            .map_err(pg_error)?
            .embedded_applied();
        if !applied {
            pending_libraries.push(library.name);
        }
    }
    let service_pending = !migrations_status(pool, service_migrator)
        .await
        .map_err(pg_error)?
        .embedded_applied();
    if engine_pending || !pending_libraries.is_empty() || service_pending {
        return Err(EngineError::MigrationsPending {
            engine: engine_pending,
            libraries: pending_libraries,
            service: service_pending,
        });
    }
    Ok(())
}

async fn derive_retention<P: crate::principal::Principal>(
    engine: &mut Engine<P>,
    readiness: &ReadinessHandle,
) -> Result<(), EngineError> {
    let subscriptions = {
        let reactions = engine
            .inbound_reactions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reactions.subscriptions()
    };
    if subscriptions.is_empty() {
        return Ok(());
    }
    match derive_message_retention(
        &engine.nats,
        &subscriptions,
        engine.config.message_retention,
    )
    .await
    {
        Ok(derived) => {
            engine.config.message_retention = derived;
            Ok(())
        }
        Err(error) => {
            readiness.set_not_ready(REASON_MESSAGE_RETENTION);
            Err(error)
        }
    }
}
