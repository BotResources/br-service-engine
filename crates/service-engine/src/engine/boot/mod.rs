mod env;
mod libraries;
mod migrate;
mod serve;

pub use libraries::LibraryMigrations;
pub use migrate::apply_migration_chain;
pub use serve::ensure_migrated;

use async_graphql::{ObjectType, SubscriptionType};
use br_util_postgres::PostgresError;
use sqlx::migrate::Migrator;

use crate::config::EngineConfig;
use crate::engine::Engine;
use crate::error::EngineError;
use crate::graphql::PassportPrincipal;

pub const MIGRATE_SUBCOMMAND: &str = "migrate";
pub const SERVE_SUBCOMMAND: &str = "serve";
pub const SCHEMA_SUBCOMMAND: &str = "schema";

pub const REASON_MIGRATIONS_PENDING: &str =
    "the store is not fully migrated; migrate must apply the engine, library and service migration \
     sets before serve runs";

pub struct BootPlan<Q, M, S, R> {
    pub component: &'static str,
    pub libraries: Vec<LibraryMigrations>,
    pub service_migrator: Migrator,
    pub config: EngineConfig,
    pub query: Q,
    pub mutation: M,
    pub subscription: S,
    pub declare_scopes: bool,
    pub register: R,
}

enum Subcommand {
    Migrate,
    Serve,
    Schema,
}

pub async fn run_service<P, Q, M, S, R>(plan: BootPlan<Q, M, S, R>) -> Result<(), EngineError>
where
    P: PassportPrincipal,
    Q: ObjectType + 'static,
    M: ObjectType + 'static,
    S: SubscriptionType + 'static,
    R: FnOnce(&mut Engine<P>) -> Result<(), EngineError>,
{
    match subcommand()? {
        Subcommand::Schema => {
            dump_schema(plan);
            Ok(())
        }
        Subcommand::Migrate => migrate::migrate(plan).await,
        Subcommand::Serve => serve::serve::<P, Q, M, S, R>(plan).await,
    }
}

fn dump_schema<Q, M, S, R>(plan: BootPlan<Q, M, S, R>)
where
    Q: ObjectType + 'static,
    M: ObjectType + 'static,
    S: SubscriptionType + 'static,
{
    let schema =
        async_graphql::Schema::build(plan.query, plan.mutation, plan.subscription).finish();
    println!("{}", schema.sdl());
}

pub(crate) fn wants_schema() -> bool {
    std::env::args().nth(1).as_deref() == Some(SCHEMA_SUBCOMMAND)
}

pub(crate) fn wants_migrate() -> bool {
    std::env::args().nth(1).as_deref() == Some(MIGRATE_SUBCOMMAND)
}

fn subcommand() -> Result<Subcommand, EngineError> {
    match std::env::args().nth(1).as_deref() {
        None | Some(SERVE_SUBCOMMAND) => Ok(Subcommand::Serve),
        Some(MIGRATE_SUBCOMMAND) => Ok(Subcommand::Migrate),
        Some(SCHEMA_SUBCOMMAND) => Ok(Subcommand::Schema),
        Some(other) => Err(EngineError::Config(format!(
            "unknown subcommand `{other}`; expected migrate, serve, or schema (no argument serves)"
        ))),
    }
}

pub(crate) fn pg_error(error: PostgresError) -> EngineError {
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
