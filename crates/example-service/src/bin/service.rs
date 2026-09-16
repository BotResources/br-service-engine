//! The reference service `main`: build the config from the environment, then hand
//! everything to the engine boot kit. Logging, `/livez` + `/metrics` + `/sdl`, the
//! `schema` subcommand, the owner→migrate→grant→app-pool sequence, and serving are all
//! the kit's job — a service's `main` only assembles configuration.

use std::net::SocketAddr;
use std::time::Duration;

use example_service::slices::{MutationRoot, QueryRoot, SubscriptionRoot};
use service_engine::config::EngineConfig;
use service_engine::name::{ChannelName, PodId};
use service_engine::{BlobConfig, BootPlan, run_service};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let channel = std::env::var("ENGINE_CHANNEL").unwrap_or_else(|_| "example".to_string());
    let pod = std::env::var("POD_ID").unwrap_or_else(|_| "pod-1".to_string());
    let app_role = std::env::var("APP_ROLE").unwrap_or_else(|_| "example_app".to_string());
    let http_addr: SocketAddr = std::env::var("HTTP_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()?;

    let mut config = EngineConfig::new(ChannelName::new(&channel)?, PodId::new(&pod)?)
        .with_service(example_contract::SERVICE)
        .with_http_addr(http_addr);

    if let Ok(ms) = std::env::var("SESSION_TTL_MS") {
        config = config.with_session_ttl(Duration::from_millis(ms.parse()?));
    }
    if let Ok(ms) = std::env::var("SESSION_MAX_AGE_MS") {
        config = config.with_session_max_age(Duration::from_millis(ms.parse()?));
    }
    if let Ok(ms) = std::env::var("ENGINE_LEASE_MS") {
        config = config.with_lease(Duration::from_millis(ms.parse()?));
    }
    if let Ok(ms) = std::env::var("ENGINE_BEAT_MS") {
        config = config.with_beat(Duration::from_millis(ms.parse()?));
    }

    if let (Ok(endpoint), Ok(bucket), Ok(access), Ok(secret)) = (
        std::env::var("S3_ENDPOINT"),
        std::env::var("S3_BUCKET"),
        std::env::var("S3_ACCESS_KEY"),
        std::env::var("S3_SECRET_KEY"),
    ) {
        let region = std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        config =
            config.with_blob_storage(BlobConfig::new(endpoint, region, bucket, access, secret));
    }

    run_service(BootPlan {
        component: "example-service",
        app_role,
        service_migrator: example_service::db::migrator(),
        config,
        nats_url,
        query: QueryRoot::default(),
        mutation: MutationRoot::default(),
        subscription: SubscriptionRoot::default(),
        declare_scopes: true,
        register: example_service::register::all,
    })
    .await?;
    Ok(())
}
