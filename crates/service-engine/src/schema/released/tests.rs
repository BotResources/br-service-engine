use std::borrow::Cow;
use std::path::{Path, PathBuf};

use sqlx::migrate::MigrationType;

use super::*;
use crate::schema::{RESERVED_VERSION_MAX, RESERVED_VERSION_MIN, migrator};

const VERSION: i64 = 9_113_000_900;

fn migration(version: i64, sql: &'static str) -> Migration {
    Migration::new(
        version,
        Cow::Borrowed("fixture"),
        MigrationType::Simple,
        Cow::Borrowed(sql),
        false,
    )
}

fn in_memory(migrations: Vec<Migration>) -> Migrator {
    Migrator {
        migrations: Cow::Owned(migrations),
        ignore_missing: true,
        locking: true,
        no_tx: false,
    }
}

#[test]
fn every_registered_checksum_is_a_released_engine_file_that_differs_from_head() {
    let migrator = migrator();
    for (version, released) in EDITED_AFTER_RELEASE {
        assert!(
            (RESERVED_VERSION_MIN..=RESERVED_VERSION_MAX).contains(version),
            "{version} is outside the engine band, so its row belongs to a library or a service"
        );
        let current = current(&migrator, *version)
            .unwrap_or_else(|| panic!("{version} is not an engine migration at HEAD"));
        assert!(!released.is_empty(), "{version} registers no checksum");
        for hex in *released {
            assert!(
                hex.len() == 96
                    && hex
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
                "{hex} is not a lowercase-hex SHA-384"
            );
            assert_ne!(
                *hex,
                crate::hex::lower(&current.checksum),
                "{version} registers its current checksum"
            );
        }
    }
}

#[test]
fn a_released_checksum_is_adopted_as_the_current_one() {
    let released = migration(VERSION, "-- a comment later removed\nSELECT 1;");
    let migrator = in_memory(vec![migration(VERSION, "SELECT 1;")]);
    let registered = [crate::hex::lower(&released.checksum)];
    let registered: Vec<&str> = registered.iter().map(String::as_str).collect();
    let registry = [(VERSION, registered.as_slice())];

    let found = adoptions(
        &[(VERSION, released.checksum.to_vec())],
        &migrator,
        &registry,
    );

    assert_eq!(
        found,
        vec![Adoption {
            version: VERSION,
            released: released.checksum.to_vec(),
            current: migration(VERSION, "SELECT 1;").checksum.to_vec(),
        }]
    );
}

#[test]
fn an_unknown_a_current_or_an_unregistered_version_is_never_adopted() {
    let released = migration(VERSION, "-- a comment later removed\nSELECT 1;");
    let current = migration(VERSION, "SELECT 1;");
    let unknown = migration(VERSION, "-- edited by hand\nSELECT 1;");
    let migrator = in_memory(vec![current.clone(), migration(VERSION + 1, "SELECT 2;")]);
    let registered = [crate::hex::lower(&released.checksum)];
    let registered: Vec<&str> = registered.iter().map(String::as_str).collect();
    let registry = [(VERSION, registered.as_slice())];

    let stored = [
        (VERSION, unknown.checksum.to_vec()),
        (VERSION, current.checksum.to_vec()),
        (VERSION + 1, released.checksum.to_vec()),
        (VERSION + 2, released.checksum.to_vec()),
    ];

    assert_eq!(adoptions(&stored, &migrator, &registry), Vec::new());
}

#[test]
fn a_released_file_breaches_only_when_it_is_gone_or_edited_without_registration() {
    let released = migration(VERSION, "-- a comment later removed\nSELECT 1;");
    let migrator = in_memory(vec![migration(VERSION, "SELECT 1;")]);
    let registered = [crate::hex::lower(&released.checksum)];
    let registered: Vec<&str> = registered.iter().map(String::as_str).collect();
    let registry = [(VERSION, registered.as_slice())];
    let current = migration(VERSION, "SELECT 1;").checksum;

    assert_eq!(
        released_file_breach(&migrator, &registry, VERSION, &current),
        None
    );
    assert_eq!(
        released_file_breach(&migrator, &registry, VERSION, &released.checksum),
        None
    );
    let edited = released_file_breach(&migrator, &[], VERSION, &released.checksum)
        .expect("an unregistered edit is a breach");
    assert!(
        edited.contains(&crate::hex::lower(&released.checksum)),
        "{edited}"
    );
    let gone = released_file_breach(&migrator, &registry, VERSION + 1, &current)
        .expect("a removed migration is a breach");
    assert!(gone.contains("gone from HEAD"), "{gone}");
}

/// The CI guard. `.github/scripts/check-released-migrations.sh` exports
/// every release tag's engine migrations as `<root>/<tag>/<file>.sql` and
/// runs this test with `ENGINE_RELEASED_MIGRATIONS=<root>`.
#[test]
#[ignore = "needs the released migration sets; run .github/scripts/check-released-migrations.sh"]
fn every_released_engine_migration_is_current_or_registered() {
    let root = std::env::var_os("ENGINE_RELEASED_MIGRATIONS").expect(
        "ENGINE_RELEASED_MIGRATIONS is unset; run .github/scripts/check-released-migrations.sh",
    );
    let migrator = migrator();
    let mut checked = 0usize;
    let mut breaches = Vec::new();
    for tag in sorted_entries(Path::new(&root)) {
        for file in sorted_entries(&tag) {
            let name = file
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_owned();
            let Some(stem) = name.strip_suffix(".sql") else {
                continue;
            };
            let version: i64 = stem
                .split('_')
                .next()
                .and_then(|prefix| prefix.parse().ok())
                .unwrap_or_else(|| panic!("{} has no numeric version", file.display()));
            let sql = std::fs::read_to_string(&file)
                .unwrap_or_else(|error| panic!("read {}: {error}", file.display()));
            let released = Migration::new(
                version,
                Cow::Owned(name.clone()),
                MigrationType::Simple,
                Cow::Owned(sql),
                false,
            );
            checked += 1;
            if let Some(breach) =
                released_file_breach(&migrator, EDITED_AFTER_RELEASE, version, &released.checksum)
            {
                breaches.push(format!("{} {name}: {breach}", tag.display()));
            }
        }
    }
    assert!(
        checked > 0,
        "no released migration found under {}",
        Path::new(&root).display()
    );
    assert!(breaches.is_empty(), "{}", breaches.join("\n"));
}

fn sorted_entries(dir: &Path) -> Vec<PathBuf> {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("read {}: {error}", dir.display()))
        .map(|entry| entry.expect("a directory entry").path())
        .collect();
    entries.sort();
    entries
}
