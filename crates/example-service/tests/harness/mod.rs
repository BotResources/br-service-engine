pub mod nats;
pub mod pg;

use std::net::SocketAddr;
use std::time::Duration;

use br_core_auth::{AuthMethod, Passport, PassportClaims, PassportHeader};
use example_service::boot::{BootOptions, Service, boot};
use service_engine::config::EngineConfig;
use service_engine::name::{ChannelName, PodId};
use service_engine::nats::Nats;
use uuid::Uuid;

pub use nats::TestNats;
pub use pg::TestDb;

pub struct World {
    pub db: TestDb,
    pub nats_server: TestNats,
    pub nats: Nats,
    pub service: Service,
    pub http: reqwest::Client,
}

impl World {
    pub async fn start(pod: &str) -> World {
        let db = TestDb::fresh().await;
        let nats_server = TestNats::spawn().await;
        nats_server.provision(example_contract::SERVICE).await;
        let nats = nats_server.nats().await;

        example_service::twin::publish_person(
            &nats,
            &example_contract::PublishedPerson {
                id: Uuid::now_v7(),
                email: "seed@example.test".to_string(),
                display_name: "Seed".to_string(),
            },
        )
        .await
        .expect("seed the roster so the directory mirror has a non-empty prefix at boot");

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = EngineConfig::new(
            ChannelName::new("example").expect("valid channel"),
            PodId::new(pod).expect("valid pod id"),
        )
        .with_service(example_contract::SERVICE)
        .with_window(Duration::from_millis(30))
        .with_beat(Duration::from_millis(80))
        .with_lease(Duration::from_secs(5))
        .with_lock_timeout(Duration::from_millis(400))
        .with_http_addr(addr);

        let service = boot(
            config,
            db.app.clone(),
            nats_server.nats().await,
            BootOptions {
                declare_scopes: false,
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
        }
    }

    pub async fn boot_second_pod(&self, pod: &str) -> Service {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = EngineConfig::new(
            ChannelName::new("example").expect("valid channel"),
            PodId::new(pod).expect("valid pod id"),
        )
        .with_service(example_contract::SERVICE)
        .with_window(Duration::from_millis(30))
        .with_beat(Duration::from_millis(80))
        .with_lease(Duration::from_secs(5))
        .with_lock_timeout(Duration::from_millis(400))
        .with_http_addr(addr);
        boot(
            config,
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

pub async fn settle() {
    tokio::time::sleep(Duration::from_millis(300)).await;
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
