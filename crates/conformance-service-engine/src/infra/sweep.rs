use sqlx::PgPool;
use sqlx::Row;
use std::time::{SystemTime, UNIX_EPOCH};

pub const NAME_PREFIX: &str = "se_";

const TIMESTAMP_HEX_LEN: usize = 12;
const STALE_AFTER_MS: u128 = 3_600_000;

pub async fn sweep_stale(admin: &PgPool) {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock after the epoch")
        .as_millis();

    let databases = sqlx::query("SELECT datname FROM pg_database WHERE datname LIKE 'se\\_%\\_db'")
        .fetch_all(admin)
        .await
        .expect("list the disposable databases of earlier runs");
    for name in databases.iter().map(|row| row.get::<String, _>("datname")) {
        if stale(&name, now_ms) {
            let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
                .execute(admin)
                .await;
        }
    }

    let roles = sqlx::query("SELECT rolname FROM pg_roles WHERE rolname LIKE 'se\\_%'")
        .fetch_all(admin)
        .await
        .expect("list the disposable roles of earlier runs");
    for name in roles.iter().map(|row| row.get::<String, _>("rolname")) {
        if stale(&name, now_ms) {
            let _ = sqlx::query(&format!("DROP ROLE IF EXISTS \"{name}\""))
                .execute(admin)
                .await;
        }
    }
}

fn stale(name: &str, now_ms: u128) -> bool {
    let Some(rest) = name.strip_prefix(NAME_PREFIX) else {
        return false;
    };
    if rest.len() < TIMESTAMP_HEX_LEN {
        return false;
    }
    let Ok(created_ms) = u128::from_str_radix(&rest[..TIMESTAMP_HEX_LEN], 16) else {
        return false;
    };
    now_ms.saturating_sub(created_ms) > STALE_AFTER_MS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_disposable_name_is_swept_only_once_it_is_older_than_the_grace_period() {
        let now = 1_800_000_000_000u128;
        let fresh = format!("{NAME_PREFIX}{now:012x}abcdefgh_db");
        let old = format!("{NAME_PREFIX}{:012x}abcdefgh_db", now - STALE_AFTER_MS - 1);
        assert!(!stale(&fresh, now));
        assert!(stale(&old, now));
        assert!(!stale("postgres", now));
        assert!(!stale("se_short", now));
    }
}
