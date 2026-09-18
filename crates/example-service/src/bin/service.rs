use example_service::slices::{MutationRoot, QueryRoot, SubscriptionRoot};
use service_engine::config::EngineConfig;
use service_engine::{BlobConfig, BootPlan, run_service};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = EngineConfig::from_env()?.with_service(example_contract::SERVICE);

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
        service_migrator: example_service::db::migrator(),
        config,
        query: QueryRoot::default(),
        mutation: MutationRoot::default(),
        subscription: SubscriptionRoot::default(),
        declare_scopes: true,
        register: example_service::register::all,
    })
    .await?;
    Ok(())
}
