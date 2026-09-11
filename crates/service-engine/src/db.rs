use std::str::FromStr;
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};

use crate::error::EngineError;

pub const TRUSTED_NETWORK_HOSTS: &str = "TRUSTED_NETWORK_HOSTS";

fn config(detail: impl Into<String>) -> EngineError {
    EngineError::Config(detail.into())
}

fn extract_pg_host(url: &str) -> String {
    let without_scheme = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))
        .unwrap_or(url);
    let after_auth = match without_scheme.find('@') {
        Some(pos) => &without_scheme[pos + 1..],
        None => without_scheme,
    };
    let host_port = after_auth.split('/').next().unwrap_or(after_auth);
    let host_port = host_port.split('?').next().unwrap_or(host_port);
    if host_port.starts_with('[') {
        return host_port
            .trim_start_matches('[')
            .split(']')
            .next()
            .unwrap_or_default()
            .to_string();
    }
    host_port.split(':').next().unwrap_or_default().to_string()
}

fn is_loopback(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

fn trusted_network_hosts() -> Vec<String> {
    std::env::var(TRUSTED_NETWORK_HOSTS)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|host| !host.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn percent_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(high), Some(low)) = (hex_value(bytes[i + 1]), hex_value(bytes[i + 2]))
        {
            out.push(high * 16 + low);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn overrides_target_host(url: &str) -> bool {
    let Some(query) = url.split('?').nth(1) else {
        return false;
    };
    let query = query.split('#').next().unwrap_or(query);
    query.split('&').any(|pair| {
        let key = pair.split('=').next().unwrap_or(pair);
        let key = percent_decode(key).to_ascii_lowercase();
        key == "host" || key == "hostaddr"
    })
}

fn ssl_mode(url: &str) -> Result<PgSslMode, EngineError> {
    PgConnectOptions::from_str(url)
        .map(|options| options.get_ssl_mode())
        .map_err(|error| {
            config(format!(
                "could not parse DATABASE_URL for TLS validation: {error}"
            ))
        })
}

pub fn validate_database_tls(url: &str) -> Result<(), EngineError> {
    validate_against(url, &trusted_network_hosts())
}

fn validate_against(url: &str, trusted: &[String]) -> Result<(), EngineError> {
    if overrides_target_host(url) {
        return Err(config(
            "DATABASE_URL overrides the target host via a host=/hostaddr= query parameter; TLS \
             validation cannot vouch for the real target — put the host in the URL authority",
        ));
    }

    let host = extract_pg_host(url);
    if is_loopback(&host) {
        return Ok(());
    }
    if trusted.iter().any(|entry| entry == &host) {
        return Ok(());
    }

    let has_tls = matches!(
        ssl_mode(url)?,
        PgSslMode::Require | PgSslMode::VerifyCa | PgSslMode::VerifyFull
    );
    if !has_tls {
        return Err(config(format!(
            "remote database connection to '{host}' requires TLS: add sslmode=require (or \
             verify-ca/verify-full) to DATABASE_URL, or declare the host in \
             {TRUSTED_NETWORK_HOSTS} if it sits on a trusted network segment"
        )));
    }
    Ok(())
}

pub async fn connect_pool(database_url: &str) -> Result<PgPool, EngineError> {
    validate_database_tls(database_url)?;
    PgPoolOptions::new()
        .max_connections(20)
        .min_connections(2)
        .acquire_timeout(Duration::from_secs(8))
        .connect(database_url)
        .await
        .map_err(EngineError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_the_host_from_the_url_authority() {
        assert_eq!(
            extract_pg_host(&format!(
                "postgres://{}:{}@{}:5432/mydb",
                "user", "pass", "db.example.com"
            )),
            "db.example.com"
        );
        assert_eq!(extract_pg_host("postgres://user@[::1]:5432/mydb"), "::1");
        assert_eq!(extract_pg_host("postgres://"), "");
    }

    #[test]
    fn loopback_needs_no_tls() {
        assert!(validate_database_tls("postgres://localhost/db").is_ok());
        assert!(validate_database_tls("postgres://user@[::1]:5432/db").is_ok());
    }

    #[test]
    fn a_remote_host_without_tls_is_refused_and_with_tls_accepted() {
        assert!(validate_database_tls("postgres://db.example.com/db").is_err());
        assert!(validate_database_tls("postgres://db.example.com/db?sslmode=disable").is_err());
        assert!(validate_database_tls("postgres://db.example.com/db?sslmode=require").is_ok());
        assert!(validate_database_tls("postgres://db.example.com/db?ssl-mode=verify-full").is_ok());
    }

    #[test]
    fn an_unparseable_or_missing_host_fails_closed() {
        assert!(validate_database_tls("postgres://db.example.com/db?sslmode=not-a-mode").is_err());
    }

    #[test]
    fn a_host_override_query_parameter_is_refused_even_percent_encoded() {
        assert!(validate_database_tls("postgres://localhost/db?host=evil.example.com").is_err());
        assert!(validate_database_tls("postgres://127.0.0.1/db?hostaddr=8.8.8.8").is_err());
        assert!(validate_database_tls("postgres://localhost/db?%68ost=evil.example.com").is_err());
        assert!(validate_database_tls("postgres://localhost/db?application_name=svc").is_ok());
    }

    #[test]
    fn a_duplicated_sslmode_that_ends_disabled_is_refused() {
        assert!(
            validate_database_tls("postgres://db.example.com/db?sslmode=require&sslmode=disable")
                .is_err()
        );
    }

    #[test]
    fn a_listed_trusted_network_host_needs_no_tls_but_an_unlisted_one_does() {
        let trusted = vec!["cnpg-rw".to_string()];
        assert!(validate_against("postgres://cnpg-rw/db", &trusted).is_ok());
        assert!(validate_against("postgres://other-db/db", &trusted).is_err());
        assert!(validate_against("postgres://other-db/db?sslmode=require", &trusted).is_ok());
    }
}
