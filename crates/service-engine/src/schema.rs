use sqlx::PgPool;
use sqlx::migrate::Migrator;

use crate::error::EngineError;

pub const SCHEMA: &str = "service_engine";

pub const RESERVED_VERSION_MIN: i64 = 9_113_000_001;
pub const RESERVED_VERSION_MAX: i64 = 9_113_999_999;

pub const TABLE_SCHEDULED_IMPACT: &str = "service_engine.scheduled_impact";
pub const TABLE_LEADER_SLOT: &str = "service_engine.leader_slot";
pub const TABLE_ACCUMULATOR_CHUNK: &str = "service_engine.accumulator_chunk";
pub const TABLE_ACCUMULATOR_SEAL: &str = "service_engine.accumulator_seal";
pub const TABLE_KV_RELAY_WATERMARK: &str = "service_engine.kv_relay_watermark";

pub const TABLES: &[&str] = &[
    TABLE_SCHEDULED_IMPACT,
    TABLE_LEADER_SLOT,
    TABLE_ACCUMULATOR_CHUNK,
    TABLE_ACCUMULATOR_SEAL,
    TABLE_KV_RELAY_WATERMARK,
];

const MAX_ROLE_NAME_LEN: usize = 63;

fn migrator() -> Migrator {
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.set_ignore_missing(true);
    migrator
}

pub async fn migrate(pool: &PgPool) -> Result<(), EngineError> {
    migrator().run(pool).await?;
    Ok(())
}

pub async fn grant_engine_access(pool: &PgPool, app_role: &str) -> Result<(), EngineError> {
    validate_role_name(app_role)?;
    for sql in [
        format!("GRANT USAGE ON SCHEMA {SCHEMA} TO \"{app_role}\""),
        format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {SCHEMA} TO \"{app_role}\""
        ),
        format!(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA {SCHEMA} \
             GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"{app_role}\""
        ),
    ] {
        sqlx::query(&sql).execute(pool).await?;
    }
    Ok(())
}

fn validate_role_name(name: &str) -> Result<(), EngineError> {
    let mut chars = name.chars();
    let first = chars.next();
    let shaped = match first {
        Some(c) if c.is_ascii_lowercase() => {
            chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        }
        _ => false,
    };
    if shaped && name.len() <= MAX_ROLE_NAME_LEN {
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
        assert_eq!(TABLES.len(), 5);
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
