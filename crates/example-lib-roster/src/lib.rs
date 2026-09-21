mod delta;
mod principal;
mod slice;
mod view;

use std::ops::RangeInclusive;

use service_engine::LibraryMigrations;

pub use delta::{RosterDelta, RosterRemove, RosterReset, RosterUpsert, RosterViewUnion};
pub use principal::RosterPrincipal;
pub use view::{KnownPerson, RosterUsers, RosterView};

pub const NAME: &str = "roster";
pub const SCHEMA: &str = "roster";
pub const BAND: RangeInclusive<i64> = 9_120_000_001..=9_120_999_999;
pub const KNOWN_PERSONS_TABLE: &str = "roster.known_persons";
pub const KNOWN_PERSON_NAMESPACE: &str = "roster.person";

pub fn migrations() -> LibraryMigrations {
    LibraryMigrations {
        name: NAME,
        schema: SCHEMA,
        band: BAND,
        migrator: sqlx::migrate!("./migrations"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_declared_migrations_sit_inside_the_declared_band() {
        let library = migrations();
        let versions: Vec<i64> = library.migrator.iter().map(|m| m.version).collect();
        assert!(!versions.is_empty(), "the roster library ships migrations");
        for version in versions {
            assert!(
                BAND.contains(&version),
                "migration {version} escapes the roster band"
            );
        }
    }
}
