use std::net::SocketAddr;
use std::time::Duration;

use crate::config::EngineConfig;
use crate::error::EngineError;
use crate::name::{ChannelName, PodId};

pub const DATABASE_URL: &str = "DATABASE_URL";
pub const DATABASE_URL_OWNER: &str = "DATABASE_URL_OWNER";

impl EngineConfig {
    pub fn from_env() -> Result<Self, EngineError> {
        if super::wants_schema() {
            return Ok(EngineConfig::new(
                ChannelName::from_static("schema"),
                PodId::from_static("schema"),
            ));
        }

        if super::wants_migrate() {
            let app_role = std::env::var("APP_ROLE")
                .map_err(|_| EngineError::Config("APP_ROLE must be set for migrate".to_string()))?;
            return Ok(EngineConfig::new(
                ChannelName::from_static("migrate"),
                PodId::from_static("migrate"),
            )
            .with_app_role(app_role));
        }

        let channel = required("ENGINE_CHANNEL")?;
        let hostname = required("HOSTNAME")?;
        let nats_url = required("NATS_URL")?;
        let app_role = required("APP_ROLE")?;

        let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
        let http_addr: SocketAddr = format!("{host}:{port}").parse().map_err(|error| {
            EngineError::Config(format!(
                "HOST `{host}` and PORT `{port}` do not form a socket address: {}",
                crate::chain::describe(&error)
            ))
        })?;

        let mut config = EngineConfig::new(ChannelName::new(channel)?, PodId::new(hostname)?)
            .with_nats_url(nats_url)
            .with_app_role(app_role)
            .with_http_addr(http_addr);

        if let Some(ttl) = millis("SESSION_TTL_MS")? {
            config = config.with_session_ttl(ttl);
        }
        if let Some(max_age) = millis("SESSION_MAX_AGE_MS")? {
            config = config.with_session_max_age(max_age);
        }
        if let Some(lease) = millis("ENGINE_LEASE_MS")? {
            config = config.with_lease(lease);
        }
        if let Some(beat) = millis("ENGINE_BEAT_MS")? {
            config = config.with_beat(beat);
        }
        Ok(config)
    }
}

fn required(key: &str) -> Result<String, EngineError> {
    std::env::var(key).map_err(|_| EngineError::Config(format!("{key} must be set for serve")))
}

fn millis(key: &str) -> Result<Option<Duration>, EngineError> {
    match std::env::var(key) {
        Ok(raw) => raw
            .parse::<u64>()
            .map(|ms| Some(Duration::from_millis(ms)))
            .map_err(|error| {
                EngineError::Config(format!(
                    "{key} is not a whole number of milliseconds: {}",
                    crate::chain::describe(&error)
                ))
            }),
        Err(_) => Ok(None),
    }
}

pub(crate) fn owner_database_url() -> Result<String, EngineError> {
    std::env::var(DATABASE_URL_OWNER).map_err(|_| {
        EngineError::Config(format!(
            "{DATABASE_URL_OWNER} must be set for migrate; there is no fallback to {DATABASE_URL}"
        ))
    })
}

pub(crate) fn app_database_url() -> Result<String, EngineError> {
    std::env::var(DATABASE_URL)
        .map_err(|_| EngineError::Config(format!("{DATABASE_URL} must be set for the app pool")))
}
