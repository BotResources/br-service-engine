use std::ops::RangeInclusive;

use service_engine::LibraryMigrations;
use sqlx::migrate::Migrator;

pub const NAME: &str = "sample_lib";
pub const SCHEMA: &str = "sample_lib";
pub const BAND: RangeInclusive<i64> = 9_120_000_001..=9_120_999_999;

pub fn migrations() -> LibraryMigrations {
    LibraryMigrations {
        name: NAME,
        schema: SCHEMA,
        band: BAND,
        migrator: sqlx::migrate!("./migrations-sample-lib"),
    }
}

pub fn dependent_service_migrator() -> Migrator {
    sqlx::migrate!("./migrations-sample-lib-service")
}
