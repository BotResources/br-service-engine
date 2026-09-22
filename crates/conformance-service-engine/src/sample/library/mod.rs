use std::borrow::Cow;
use std::ops::RangeInclusive;

use service_engine::LibraryMigrations;
use sqlx::migrate::{Migration, MigrationType, Migrator};

pub const NAME: &str = "sample_lib";
pub const SCHEMA: &str = "sample_lib";
pub const BAND: RangeInclusive<i64> = 9_120_000_001..=9_120_999_999;
pub const ITEM_VERSION: i64 = 9_120_000_001;
pub const BASE_SERVICE_VERSION: i64 = 20_260_921_000_300;

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

pub fn escaping_migrations() -> LibraryMigrations {
    LibraryMigrations {
        name: NAME,
        schema: SCHEMA,
        band: BAND,
        migrator: in_memory(vec![Migration::new(
            ITEM_VERSION,
            Cow::Borrowed("sample_lib_escape"),
            MigrationType::Simple,
            Cow::Borrowed("CREATE TABLE public.sample_lib_escapee (id uuid PRIMARY KEY)"),
            false,
        )]),
    }
}

pub fn base_service_migrator() -> Migrator {
    in_memory(vec![Migration::new(
        BASE_SERVICE_VERSION,
        Cow::Borrowed("sample_base"),
        MigrationType::Simple,
        Cow::Borrowed("CREATE TABLE IF NOT EXISTS sample_base_pin (id uuid PRIMARY KEY)"),
        false,
    )])
}

pub fn empty_service_migrator() -> Migrator {
    in_memory(Vec::new())
}

fn in_memory(migrations: Vec<Migration>) -> Migrator {
    Migrator {
        migrations: Cow::Owned(migrations),
        ignore_missing: true,
        locking: true,
        no_tx: false,
    }
}
