use sqlx::migrate::{Migrate, Migrator};
use sqlx::{PgConnection, PgPool};

use crate::error::EngineError;

mod released;

pub const SCHEMA: &str = "service_engine";

pub const ENGINE_SCHEMA_VERSION: &str = env!("CARGO_PKG_VERSION");

pub const RESERVED_VERSION_MIN: i64 = 9_113_000_001;
pub const RESERVED_VERSION_MAX: i64 = 9_113_999_999;

pub const TABLE_SCHEDULED_IMPACT: &str = "service_engine.scheduled_impact";
pub const TABLE_LEADER_SLOT: &str = "service_engine.leader_slot";
pub const TABLE_ACCUMULATOR_CHUNK: &str = "service_engine.accumulator_chunk";
pub const TABLE_ACCUMULATOR_SEAL: &str = "service_engine.accumulator_seal";
pub const TABLE_KV_RELAY_WATERMARK: &str = "service_engine.kv_relay_watermark";
pub const TABLE_MESSAGE_CLAIM: &str = "service_engine.message_claim";
pub const TABLE_SEQUENCE_GUARD: &str = "service_engine.sequence_guard";
pub const TABLE_DEAD_LETTER: &str = "service_engine.dead_letter";
pub const TABLE_SCHEDULED_MESSAGE: &str = "service_engine.scheduled_message";
pub const TABLE_OFFER_DIRTY: &str = "service_engine.offer_dirty";
pub const TABLE_BLOB: &str = "service_engine.blob";
pub const TABLE_PERSON_ERASURE: &str = "service_engine.person_erasure";
pub const TABLE_SCHEMA_VERSION: &str = "service_engine.schema_version";
pub const TABLE_EVENT_LOG: &str = "service_engine.event_log";
pub const TABLE_EVENT_SNAPSHOT: &str = "service_engine.event_snapshot";
pub const TABLE_MIRROR_WATERMARK: &str = "service_engine.mirror_watermark";
pub const TABLE_INTEGRATION_OUTBOX: &str = "service_engine.integration_outbox";

pub const TABLES: &[&str] = &[
    TABLE_SCHEDULED_IMPACT,
    TABLE_LEADER_SLOT,
    TABLE_ACCUMULATOR_CHUNK,
    TABLE_ACCUMULATOR_SEAL,
    TABLE_KV_RELAY_WATERMARK,
    TABLE_MESSAGE_CLAIM,
    TABLE_SEQUENCE_GUARD,
    TABLE_DEAD_LETTER,
    TABLE_SCHEDULED_MESSAGE,
    TABLE_OFFER_DIRTY,
    TABLE_BLOB,
    TABLE_PERSON_ERASURE,
    TABLE_SCHEMA_VERSION,
    TABLE_EVENT_LOG,
    TABLE_EVENT_SNAPSHOT,
    TABLE_MIRROR_WATERMARK,
    TABLE_INTEGRATION_OUTBOX,
];

const MAX_ROLE_NAME_LEN: usize = 63;

pub(crate) fn migrator() -> Migrator {
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.set_ignore_missing(true);
    migrator
}

/// Applies the engine set. Under the migrator's own advisory lock, the stored
/// checksum of an engine migration a released engine applied with other bytes
/// is first adopted (see `released`); any other checksum difference still fails
/// as sqlx's `VersionMismatch`.
pub async fn migrate(pool: &PgPool) -> Result<(), EngineError> {
    let mut conn = pool.acquire().await?;
    let migrated = migrate_locked(&mut conn).await;
    if migrated.is_err() {
        // A failed run can leave the session-level advisory lock held; closing
        // the session releases it instead of pooling a locked connection.
        let _ = conn.close().await;
    }
    migrated
}

async fn migrate_locked(conn: &mut PgConnection) -> Result<(), EngineError> {
    let migrator = migrator();
    conn.lock().await?;
    released::adopt_released_checksums(conn, &migrator).await?;
    migrator.run(&mut *conn).await?;
    conn.unlock().await?;
    Ok(())
}

pub async fn grant_engine_access(pool: &PgPool, app_role: &str) -> Result<(), EngineError> {
    grant_schema_access(pool, SCHEMA, app_role).await
}

pub async fn grant_schema_access(
    pool: &PgPool,
    schema: &str,
    app_role: &str,
) -> Result<(), EngineError> {
    validate_role_name(app_role)?;
    if !validate_pg_identifier(schema) {
        return Err(EngineError::InvalidSchemaName(schema.to_string()));
    }
    for sql in [
        format!("GRANT USAGE ON SCHEMA {schema} TO \"{app_role}\""),
        format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO \"{app_role}\""
        ),
        format!(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} \
             GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"{app_role}\""
        ),
        format!("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {schema} TO \"{app_role}\""),
        format!(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} \
             GRANT USAGE, SELECT ON SEQUENCES TO \"{app_role}\""
        ),
    ] {
        sqlx::query(&sql).execute(pool).await?;
    }
    Ok(())
}

pub(crate) fn validate_pg_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let shaped = match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {
            chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        }
        _ => false,
    };
    shaped && name.len() <= MAX_ROLE_NAME_LEN
}

fn validate_role_name(name: &str) -> Result<(), EngineError> {
    if validate_pg_identifier(name) {
        Ok(())
    } else {
        Err(EngineError::InvalidRoleName(name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_engine_migration_sits_inside_the_reserved_version_range() {
        let versions: Vec<i64> = migrator().iter().map(|m| m.version).collect();
        assert!(!versions.is_empty(), "the engine ships migrations");
        for version in &versions {
            assert!(
                (RESERVED_VERSION_MIN..=RESERVED_VERSION_MAX).contains(version),
                "migration {version} escapes the reserved range"
            );
        }
    }

    #[test]
    fn the_one_way_to_apply_the_engine_set_tolerates_a_shared_ledger() {
        assert!(migrator().ignore_missing);
    }

    #[test]
    fn every_engine_table_lives_in_the_engines_own_schema() {
        assert_eq!(TABLES.len(), 17);
        for table in TABLES {
            assert!(table.starts_with(&format!("{SCHEMA}.")));
        }
    }

    #[test]
    fn a_role_name_that_could_break_out_of_the_grant_statement_is_refused() {
        assert!(validate_role_name("sample_app").is_ok());
        assert!(validate_role_name("").is_err());
        assert!(validate_role_name("Sample").is_err());
        assert!(validate_role_name("sample\"; DROP SCHEMA service_engine; --").is_err());
        assert!(validate_role_name(&"a".repeat(64)).is_err());
    }
}
