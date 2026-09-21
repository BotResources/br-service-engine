use std::ops::RangeInclusive;

use sqlx::migrate::Migrator;

use crate::error::EngineError;
use crate::schema::{RESERVED_VERSION_MAX, RESERVED_VERSION_MIN, SCHEMA, validate_pg_identifier};

pub struct LibraryMigrations {
    pub name: &'static str,
    pub schema: &'static str,
    pub band: RangeInclusive<i64>,
    pub migrator: Migrator,
}

pub(crate) fn engine_band() -> RangeInclusive<i64> {
    RESERVED_VERSION_MIN..=RESERVED_VERSION_MAX
}

pub(crate) fn validate(
    libraries: &[LibraryMigrations],
    service_migrator: &Migrator,
) -> Result<(), EngineError> {
    for library in libraries {
        if !validate_pg_identifier(library.schema)
            || library.schema == "public"
            || library.schema == SCHEMA
        {
            return Err(EngineError::InvalidSchemaName(library.schema.to_string()));
        }
    }

    for (i, library) in libraries.iter().enumerate() {
        for other in &libraries[i + 1..] {
            if library.name == other.name || library.schema == other.schema {
                return Err(EngineError::DuplicateLibrary {
                    name: library.name.to_string(),
                });
            }
        }
    }

    let mut bands: Vec<(&'static str, RangeInclusive<i64>)> = vec![("service_engine", engine_band())];
    for library in libraries {
        bands.push((library.name, library.band.clone()));
    }
    for (i, (first, first_band)) in bands.iter().enumerate() {
        for (second, second_band) in &bands[i + 1..] {
            if first_band.start() <= second_band.end() && second_band.start() <= first_band.end() {
                return Err(EngineError::MigrationBandOverlap {
                    first,
                    second,
                });
            }
        }
    }

    for library in libraries {
        for migration in library.migrator.iter() {
            if !library.band.contains(&migration.version) {
                return Err(EngineError::MigrationOutsideBand {
                    owner: library.name,
                    version: migration.version,
                });
            }
        }
    }

    for migration in service_migrator.iter() {
        for (owner, band) in &bands {
            if band.contains(&migration.version) {
                return Err(EngineError::ServiceMigrationInReservedBand {
                    version: migration.version,
                    owner,
                });
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use sqlx::migrate::{Migration, MigrationType};

    use super::*;

    fn migrator(versions: &[i64]) -> Migrator {
        let migrations: Vec<Migration> = versions
            .iter()
            .map(|&version| {
                Migration::new(
                    version,
                    Cow::Borrowed("fixture"),
                    MigrationType::Simple,
                    Cow::Borrowed("SELECT 1"),
                    false,
                )
            })
            .collect();
        Migrator {
            migrations: Cow::Owned(migrations),
            ignore_missing: true,
            locking: true,
            no_tx: false,
        }
    }

    fn library(name: &'static str, schema: &'static str, band: RangeInclusive<i64>) -> LibraryMigrations {
        let versions: Vec<i64> = vec![*band.start()];
        LibraryMigrations {
            name,
            schema,
            migrator: migrator(&versions),
            band,
        }
    }

    #[test]
    fn a_clean_declaration_of_one_library_validates() {
        let libs = vec![library("drive", "drive", 9_120_000_001..=9_120_999_999)];
        let service = migrator(&[20_260_909_000_500]);
        assert!(validate(&libs, &service).is_ok());
    }

    #[test]
    fn a_band_that_touches_the_engine_reserved_range_is_refused() {
        let libs = vec![library("drive", "drive", RESERVED_VERSION_MIN..=RESERVED_VERSION_MAX)];
        let service = migrator(&[20_260_909_000_500]);
        assert!(matches!(
            validate(&libs, &service),
            Err(EngineError::MigrationBandOverlap {
                first: "service_engine",
                second: "drive"
            })
        ));
    }

    #[test]
    fn two_libraries_with_overlapping_bands_name_both() {
        let libs = vec![
            library("drive", "drive", 9_120_000_001..=9_120_999_999),
            library("roster", "roster", 9_120_500_000..=9_121_000_000),
        ];
        let service = migrator(&[20_260_909_000_500]);
        assert!(matches!(
            validate(&libs, &service),
            Err(EngineError::MigrationBandOverlap {
                first: "drive",
                second: "roster"
            })
        ));
    }

    #[test]
    fn a_library_migration_outside_its_band_is_refused() {
        let mut libs = vec![library("drive", "drive", 9_120_000_001..=9_120_999_999)];
        libs[0].migrator = migrator(&[9_120_000_001, 9_121_000_000]);
        let service = migrator(&[20_260_909_000_500]);
        assert!(matches!(
            validate(&libs, &service),
            Err(EngineError::MigrationOutsideBand {
                owner: "drive",
                version: 9_121_000_000
            })
        ));
    }

    #[test]
    fn a_service_migration_inside_a_reserved_band_is_refused() {
        let libs = vec![library("drive", "drive", 9_120_000_001..=9_120_999_999)];
        let service = migrator(&[9_120_000_050]);
        assert!(matches!(
            validate(&libs, &service),
            Err(EngineError::ServiceMigrationInReservedBand {
                version: 9_120_000_050,
                owner: "drive"
            })
        ));
    }

    #[test]
    fn a_service_migration_inside_the_engine_band_is_refused() {
        let libs = vec![];
        let service = migrator(&[RESERVED_VERSION_MIN]);
        assert!(matches!(
            validate(&libs, &service),
            Err(EngineError::ServiceMigrationInReservedBand {
                owner: "service_engine",
                ..
            })
        ));
    }

    #[test]
    fn a_schema_that_shadows_public_or_the_engine_is_refused() {
        let service = migrator(&[20_260_909_000_500]);
        for schema in ["public", "service_engine", "Bad-Name", "9lib"] {
            let libs = vec![library("lib", schema, 9_120_000_001..=9_120_999_999)];
            assert!(matches!(
                validate(&libs, &service),
                Err(EngineError::InvalidSchemaName(_))
            ));
        }
    }

    #[test]
    fn two_libraries_that_share_a_name_are_refused() {
        let libs = vec![
            library("drive", "drive", 9_120_000_001..=9_120_999_999),
            library("drive", "roster", 9_121_000_001..=9_121_999_999),
        ];
        let service = migrator(&[20_260_909_000_500]);
        assert!(matches!(
            validate(&libs, &service),
            Err(EngineError::DuplicateLibrary { name }) if name == "drive"
        ));
    }
}
