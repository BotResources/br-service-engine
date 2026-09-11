#![allow(dead_code)]
#![allow(unused_imports)]

pub mod bin;
pub mod pg;
pub mod scopes;

#[path = "../graphql_support/mod.rs"]
pub mod graphql_support;

use std::time::Duration;

use br_core_auth::{AuthMethod, Passport, PassportClaims, PassportHeader};
use conformance_service_engine::infra::TestNats;
use example_contract::{PublishedPerson, SERVICE};
use service_engine::nats::Nats;
use sqlx::PgPool;
use uuid::Uuid;

use bin::{SpawnEnv, Spawned, free_port};
use pg::BlackboxDb;
use scopes::{ScopeIdentity, accept_scopes};

pub use bin::SessionBounds;
pub use graphql_support::{GraphqlWs, post_json};

pub struct World {
    pub db: BlackboxDb,
    pub nats_server: TestNats,
    pub nats: Nats,
    pub service: Spawned,
    scopes: ScopeIdentity,
}

impl World {
    pub async fn start(pod: &str) -> World {
        World::start_with_bounds(pod, None).await
    }

    pub async fn start_with_bounds(pod: &str, session_bounds: Option<SessionBounds>) -> World {
        let db = BlackboxDb::fresh().await;
        let nats_server = TestNats::spawn().await;
        nats_server.provision().await;
        nats_server
            .provision_presence(SERVICE, Duration::from_secs(300))
            .await;
        nats_server
            .provision_streaming(SERVICE, Duration::from_secs(300))
            .await;
        let nats = nats_server.nats().await;

        example_twin::publish_person(
            &nats,
            &PublishedPerson {
                id: Uuid::now_v7(),
                email: "seed@example.test".to_string(),
                display_name: "Seed".to_string(),
            },
        )
        .await
        .expect("seed the roster so the directory mirror converges to a non-empty prefix at boot");

        let scopes = accept_scopes(&nats).await;

        let service = Spawned::service(SpawnEnv {
            database_url: db.app_url.clone(),
            nats_url: nats_server.url(),
            pod: pod.to_string(),
            port: free_port(),
            blobs: None,
            session_bounds,
        })
        .await;

        World {
            db,
            nats_server,
            nats,
            service,
            scopes,
        }
    }

    pub fn base_url(&self) -> &str {
        self.service.base_url()
    }

    pub async fn spawn_pod(&self, pod: &str) -> Spawned {
        Spawned::service(SpawnEnv {
            database_url: self.db.app_url.clone(),
            nats_url: self.nats_server.url(),
            pod: pod.to_string(),
            port: free_port(),
            blobs: None,
            session_bounds: None,
        })
        .await
    }

    pub async fn gql(
        &self,
        passport: &str,
        query: &str,
        variables: serde_json::Value,
    ) -> serde_json::Value {
        self.gql_at(self.base_url(), passport, query, variables)
            .await
    }

    pub async fn gql_at(
        &self,
        base_url: &str,
        passport: &str,
        query: &str,
        variables: serde_json::Value,
    ) -> serde_json::Value {
        let (_status, body) = post_json(base_url, Some(passport), query, variables).await;
        body
    }

    pub fn scope_declarations_seen(&self) -> usize {
        self.scopes.declarations_seen()
    }

    pub async fn cleanup(self) {
        self.service.shutdown().await;
        drop(self.scopes);
        drop(self.nats_server);
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

pub async fn seed_chunks(pool: &PgPool, accumulator: &str, key: Uuid, chunks: &[&str]) {
    for (seq, chunk) in chunks.iter().enumerate() {
        sqlx::query(
            "INSERT INTO service_engine.accumulator_chunk (accumulator, key, seq, chunk) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(accumulator)
        .bind(serde_json::json!(key))
        .bind(seq as i64)
        .bind(serde_json::json!(chunk))
        .execute(pool)
        .await
        .expect(
            "seed an accumulator chunk into the engine store, byte-identical to the flush path",
        );
    }
}
