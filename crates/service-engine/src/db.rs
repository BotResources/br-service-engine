use sqlx::postgres::PgPool;

use crate::engine::boot::pg_error;
use crate::error::EngineError;

pub const TRUSTED_NETWORK_HOSTS: &str = br_util_postgres::env::TRUSTED_NETWORK_HOSTS;

pub fn validate_database_tls(url: &str) -> Result<(), EngineError> {
    br_util_postgres::validate_database_tls(url).map_err(pg_error)
}

pub async fn connect_pool(database_url: &str) -> Result<PgPool, EngineError> {
    br_util_postgres::init_pool(database_url)
        .await
        .map_err(pg_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refused(url: &str) -> String {
        match validate_database_tls(url) {
            Err(EngineError::Config(message)) => message,
            other => panic!("{url} must be refused as a configuration error, got {other:?}"),
        }
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
    fn an_unparseable_sslmode_fails_closed() {
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
    fn the_host_is_read_from_the_url_authority_only() {
        let message = refused("postgres://db.remote.example/app?options=@127.0.0.1");
        assert!(message.contains("db.remote.example"), "{message}");
    }

    #[test]
    fn a_dsn_with_no_host_in_the_authority_is_refused() {
        let message = refused("postgres:///db?sslmode=require");
        assert!(message.contains("no host"), "{message}");
    }

    #[test]
    fn a_dsn_with_a_foreign_scheme_is_refused() {
        assert!(validate_database_tls("mysql://localhost/db").is_err());
    }
}
