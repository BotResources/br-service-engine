pub mod minio;
pub mod nats;
pub mod pg;
pub mod scopes;
pub mod ws;

use std::net::SocketAddr;
use std::time::Duration;

use br_core_auth::{AuthMethod, Passport, PassportClaims, PassportHeader};
use example_service::boot::{BootOptions, Service, boot};
use service_engine::config::EngineConfig;
use service_engine::name::{ChannelName, PodId};
use service_engine::nats::Nats;
use uuid::Uuid;

pub use minio::TestMinio;
pub use nats::TestNats;
pub use pg::TestDb;
pub use scopes::ScopeIdentity;
pub use ws::Subscription;

pub struct World {
    pub db: TestDb,
    pub nats_server: TestNats,
    pub nats: Nats,
    pub service: Service,
    pub http: reqwest::Client,
    pub minio: Option<TestMinio>,
    pub blob_bucket: Option<String>,
    scopes: Option<ScopeIdentity>,
}

pub struct WorldOptions {
    pub blobs: bool,
    pub declare_scopes: bool,
}

impl World {
    pub async fn start(pod: &str) -> World {
        World::start_with(
            pod,
            WorldOptions {
                blobs: false,
                declare_scopes: false,
            },
        )
        .await
    }

    pub async fn start_with(pod: &str, options: WorldOptions) -> World {
        let db = TestDb::fresh().await;
        let nats_server = TestNats::spawn().await;
        nats_server.provision(example_contract::SERVICE).await;
        let nats = nats_server.nats().await;

        example_twin::publish_person(
            &nats,
            &example_contract::PublishedPerson {
                id: Uuid::now_v7(),
                email: "seed@example.test".to_string(),
                display_name: "Seed".to_string(),
            },
        )
        .await
        .expect("seed the roster so the directory mirror has a non-empty prefix at boot");

        let (minio, blob_bucket, blob_config) = if options.blobs {
            let minio = TestMinio::spawn().await;
            let bucket = format!("ex-blobs-{}", Uuid::now_v7().simple());
            minio.create_bucket(&bucket).await;
            let config = minio.config(&bucket);
            (Some(minio), Some(bucket), Some(config))
        } else {
            (None, None, None)
        };

        let scopes = if options.declare_scopes {
            Some(scopes::accept_scopes(&nats).await)
        } else {
            None
        };

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut config = base_config(pod, addr);
        if let Some(blob_config) = blob_config {
            config = config.with_blob_storage(blob_config);
        }

        let service = boot(
            config,
            db.app.clone(),
            nats_server.nats().await,
            BootOptions {
                declare_scopes: options.declare_scopes,
                await_ready: true,
            },
        )
        .await
        .expect("the example service boots and reaches readiness");

        World {
            db,
            nats_server,
            nats,
            service,
            http: reqwest::Client::new(),
            minio,
            blob_bucket,
            scopes,
        }
    }

    pub fn scope_identity(&self) -> &ScopeIdentity {
        self.scopes
            .as_ref()
            .expect("this world was started declaring scopes")
    }

    pub async fn boot_second_pod(&self, pod: &str) -> Service {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        boot(
            base_config(pod, addr),
            self.db.app.clone(),
            self.nats_server.nats().await,
            BootOptions {
                declare_scopes: false,
                await_ready: true,
            },
        )
        .await
        .expect("the second pod boots")
    }

    pub fn subscription_url(&self) -> String {
        self.service.ws("/graphql/ws")
    }

    pub async fn gql(
        &self,
        passport: &str,
        query: &str,
        variables: serde_json::Value,
    ) -> serde_json::Value {
        self.gql_at(&self.service, passport, query, variables).await
    }

    pub async fn gql_at(
        &self,
        service: &Service,
        passport: &str,
        query: &str,
        variables: serde_json::Value,
    ) -> serde_json::Value {
        let response = self
            .http
            .post(service.http("/graphql"))
            .header("x-passport", passport)
            .json(&serde_json::json!({ "query": query, "variables": variables }))
            .send()
            .await
            .expect("the graphql request reaches the service");
        response.json().await.expect("a json graphql response")
    }

    pub async fn cleanup(self) {
        self.service.shutdown().await;
        self.db.cleanup().await;
    }
}

fn base_config(pod: &str, addr: SocketAddr) -> EngineConfig {
    EngineConfig::new(
        ChannelName::new("example").expect("valid channel"),
        PodId::new(pod).expect("valid pod id"),
    )
    .with_service(example_contract::SERVICE)
    .with_window(Duration::from_millis(30))
    .with_beat(Duration::from_millis(80))
    .with_lease(Duration::from_secs(5))
    .with_lock_timeout(Duration::from_millis(400))
    .with_http_addr(addr)
}

pub fn passport(user: Uuid, org: Uuid, scopes: &[&str], super_admin: bool) -> String {
    let mut map = serde_json::Map::new();
    map.insert(
        "org".to_string(),
        serde_json::Value::String(org.to_string()),
    );
    map.insert(
        "scopes".to_string(),
        serde_json::Value::Array(
            scopes
                .iter()
                .map(|s| serde_json::Value::String((*s).to_string()))
                .collect(),
        ),
    );
    Passport::human(
        user,
        super_admin,
        true,
        AuthMethod::Jwt,
        None,
        PassportClaims::from_map(map),
    )
    .to_header()
}

#[macro_export]
macro_rules! poll_until {
    ($within:expr, $probe:block) => {{
        let deadline = std::time::Instant::now() + $within;
        loop {
            if let Some(value) = $probe {
                break value;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the observed effect never appeared within {:?}",
                $within
            );
            tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        }
    }};
}

pub fn ok(response: &serde_json::Value) -> &serde_json::Value {
    assert!(
        response.get("errors").is_none(),
        "unexpected graphql errors: {response}"
    );
    &response["data"]
}

pub fn error_code(response: &serde_json::Value) -> String {
    response["errors"][0]["extensions"]["code"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}
