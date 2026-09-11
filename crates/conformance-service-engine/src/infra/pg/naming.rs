use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

use sqlx::postgres::PgConnectOptions;
use uuid::Uuid;

static DISPOSABLE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(super) fn disposable_suffix() -> String {
    let stamped = Uuid::now_v7().simple().to_string();
    let sequence = DISPOSABLE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!(
        "{stamped}{:04x}{:04x}",
        std::process::id() as u16,
        sequence as u16
    )
}

pub(super) fn describe(url: &str) -> String {
    match PgConnectOptions::from_str(url) {
        Ok(options) => format!(
            "{}@{}:{}/{}",
            options.get_username(),
            options.get_host(),
            options.get_port(),
            options.get_database().unwrap_or_default()
        ),
        Err(_) => "an unparseable connection url".to_string(),
    }
}

pub(super) fn url_for(admin_url: &str, role: &str, password: &str, database: &str) -> String {
    let options = PgConnectOptions::from_str(admin_url).expect("the admin url parses");
    let host = options.get_host();
    let port = options.get_port();
    format!("postgresql://{role}:{password}@{host}:{port}/{database}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suffixes_minted_inside_one_millisecond_are_all_distinct() {
        let minted: Vec<String> = (0..2_000).map(|_| disposable_suffix()).collect();
        let unique: std::collections::BTreeSet<&String> = minted.iter().collect();
        assert_eq!(
            unique.len(),
            minted.len(),
            "two scenarios minted in one millisecond would race for the same role name"
        );
        for name in &minted {
            assert_eq!(name.len(), 40);
            assert!(u128::from_str_radix(&name[..12], 16).is_ok());
        }
    }

    #[test]
    fn a_connection_url_is_described_without_its_password() {
        let secret = "hunter2";
        let described = describe(&format!(
            "postgresql://someone:{secret}@127.0.0.1:5432/engine_db"
        ));
        assert!(!described.contains(secret));
        assert_eq!(described, "someone@127.0.0.1:5432/engine_db");
    }
}
