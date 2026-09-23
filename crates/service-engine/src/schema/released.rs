//! Engine migrations whose bytes changed after a release applied them.
//!
//! sqlx stores the SHA-384 of every applied file in `_sqlx_migrations` and
//! refuses a ledger whose checksum differs from the embedded file
//! (`MigrateError::VersionMismatch`). A released engine migration is therefore
//! immutable. When one was edited anyway, the SHA-384 of every released
//! version of the file is registered in [`EDITED_AFTER_RELEASE`], and
//! `migrate` rewrites exactly those stored checksums to the current one before
//! the migrator runs. Any other difference still fails as `VersionMismatch`.
//! Only the versions listed here are read or written, all inside the engine's
//! reserved band, so the rows of libraries and services that share the ledger
//! are never touched.
//!
//! CI (`.github/scripts/check-released-migrations.sh`) exports every release
//! tag's engine migrations and fails when a released file is gone or differs
//! from HEAD without its released checksum registered here.

use sqlx::migrate::{Migrate, Migration, Migrator};
use sqlx::{Connection, PgConnection};

use crate::error::EngineError;

/// `(version, [SHA-384 of every released version of the file other than the
/// current one, lowercase hex])`.
const EDITED_AFTER_RELEASE: &[(i64, &[&str])] = &[(
    9_113_000_023,
    &[
        // v0.2.0 (f65d403e); v0.3.0 (e667eb9) removed its four-line header comment.
        "74808ac1a04d1a2f42f58ae98809c07b05fc2842661251a538e4bbececef5eaf7bc5a20c15b0f41dab6184a45daae46f",
    ],
)];

#[derive(Debug, PartialEq, Eq)]
struct Adoption {
    version: i64,
    released: Vec<u8>,
    current: Vec<u8>,
}

/// Rewrites every stored engine checksum that a release registered here to the
/// checksum of the embedded file. The caller holds the migrator's advisory lock.
pub(super) async fn adopt_released_checksums(
    conn: &mut PgConnection,
    migrator: &Migrator,
) -> Result<(), EngineError> {
    conn.ensure_migrations_table().await?;
    let versions: Vec<i64> = EDITED_AFTER_RELEASE
        .iter()
        .map(|(version, _)| *version)
        .collect();
    let mut tx = conn.begin().await?;
    let stored: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version, checksum FROM _sqlx_migrations WHERE version = ANY($1)")
            .bind(&versions)
            .fetch_all(&mut *tx)
            .await?;
    let adoptions = adoptions(&stored, migrator, EDITED_AFTER_RELEASE);
    for adoption in &adoptions {
        sqlx::query(
            "UPDATE _sqlx_migrations SET checksum = $1 WHERE version = $2 AND checksum = $3",
        )
        .bind(&adoption.current)
        .bind(adoption.version)
        .bind(&adoption.released)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    for adoption in adoptions {
        tracing::info!(
            version = adoption.version,
            old = %crate::hex::lower(&adoption.released),
            new = %crate::hex::lower(&adoption.current),
            "migrate adopted the current checksum of an engine migration that a released \
             engine applied with other bytes"
        );
    }
    Ok(())
}

fn current(migrator: &Migrator, version: i64) -> Option<&Migration> {
    migrator.iter().find(|migration| {
        migration.version == version && !migration.migration_type.is_down_migration()
    })
}

fn is_registered(registry: &[(i64, &[&str])], version: i64, checksum: &[u8]) -> bool {
    let hex = crate::hex::lower(checksum);
    registry
        .iter()
        .any(|(registered, released)| *registered == version && released.contains(&hex.as_str()))
}

fn adoptions(
    stored: &[(i64, Vec<u8>)],
    migrator: &Migrator,
    registry: &[(i64, &[&str])],
) -> Vec<Adoption> {
    stored
        .iter()
        .filter_map(|(version, checksum)| {
            let current = current(migrator, *version)?;
            (current.checksum.as_ref() != checksum.as_slice()
                && is_registered(registry, *version, checksum))
            .then(|| Adoption {
                version: *version,
                released: checksum.clone(),
                current: current.checksum.to_vec(),
            })
        })
        .collect()
}

/// Why a file a release shipped would fail `migrate` on the databases that
/// release migrated, or `None` when HEAD still accepts it. The CI guard below
/// is its one caller.
#[cfg(test)]
fn released_file_breach(
    migrator: &Migrator,
    registry: &[(i64, &[&str])],
    version: i64,
    released: &[u8],
) -> Option<String> {
    let hex = crate::hex::lower(released);
    match current(migrator, version) {
        None => Some(format!(
            "engine migration {version} was released and is gone from HEAD; a released engine \
             migration is never removed or renumbered"
        )),
        Some(current) if current.checksum.as_ref() == released => None,
        Some(_) if is_registered(registry, version, released) => None,
        Some(_) => Some(format!(
            "engine migration {version} differs from its released bytes (SHA-384 {hex}); \
             restore the released bytes, or register {hex} for {version} in \
             EDITED_AFTER_RELEASE so migrate adopts it"
        )),
    }
}

#[cfg(test)]
mod tests;
