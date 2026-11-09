use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestMinio, TestNats};
use conformance_service_engine::sample::principal::SamplePrincipal;
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{
    AttachVerifiedDoc, PROBE_SUBJECT_PREFIX, PromoteDoc, PromotePolicy, boot_blob_promote_engine,
};
use service_engine::pipeline::MutationExecutor;
use service_engine::{BlobPolicy, BlobPromotion, Engine, OneShot, UploadUrl};
use sqlx::PgPool;
use uuid::Uuid;

use crate::blob_support::post_upload;
use crate::engine_twin::await_ready;

pub const PAYLOAD: &[u8] = b"verified-bytes";
pub const SAME_LENGTH_OTHER_BYTES: &[u8] = b"tampered-bytes";
pub const SHA256_HEX: &str = "35b1135247e25b36525c4f5bb88038cf310f05ab54aa450e406e9ef5c5fde911";
pub const NO_REAPER: Duration = Duration::from_secs(3600);

pub struct Rig {
    pub db: TestDb,
    pub nats: TestNats,
    pub minio: TestMinio,
    pub bucket: String,
    pub pool: PgPool,
    pub tenant: Uuid,
    pub http: reqwest::Client,
}

impl Rig {
    pub async fn new(name: &str) -> Self {
        let db = TestDb::fresh().await;
        let nats = TestNats::spawn().await;
        nats.provision().await;
        let minio = TestMinio::spawn().await;
        let bucket = format!("se-{name}-{}", Uuid::now_v7().simple());
        minio.create_bucket(&bucket).await;
        let pool = db.app_pool().clone();
        Self {
            db,
            nats,
            minio,
            bucket,
            pool,
            tenant: Uuid::now_v7(),
            http: reqwest::Client::new(),
        }
    }

    pub async fn engine(&self, name: &str, policy: PromotePolicy) -> Engine<SamplePrincipal> {
        self.engine_with_reaper(name, policy, NO_REAPER).await
    }

    pub async fn engine_with_reaper(
        &self,
        name: &str,
        policy: PromotePolicy,
        reaper_interval: Duration,
    ) -> Engine<SamplePrincipal> {
        boot_blob_promote_engine(
            &self.db,
            self.nats.nats().await,
            &format!("se_{name}"),
            &format!("pod-{name}"),
            self.minio.config(&self.bucket),
            BlobPolicy {
                max_bytes: 1 << 20,
                orphan_after: Duration::from_secs(3600),
            },
            reaper_interval,
            policy,
        )
        .await
    }

    pub async fn principal(&self) -> SamplePrincipal {
        member(&self.pool, Uuid::now_v7(), self.tenant).await
    }

    pub async fn attach(
        &self,
        executor: &MutationExecutor<SamplePrincipal>,
    ) -> (Uuid, Uuid, UploadUrl) {
        let doc = Uuid::now_v7();
        let upload: OneShot<UploadUrl> = executor
            .run::<AttachVerifiedDoc>(
                self.principal().await,
                AttachVerifiedDoc {
                    id: doc,
                    tenant: self.tenant,
                    name: "promote.bin".to_string(),
                    content_type: "application/octet-stream".to_string(),
                    size: PAYLOAD.len() as u64,
                    sha256_hex: SHA256_HEX.to_string(),
                },
            )
            .await
            .expect("the verified attach commits");
        let reference: Uuid = sqlx::query_scalar("SELECT blob_ref FROM sample_doc WHERE id = $1")
            .bind(doc)
            .fetch_one(&self.pool)
            .await
            .expect("read the blob reference");
        (doc, reference, upload.into_inner())
    }

    pub async fn land(&self, upload: UploadUrl) {
        let status = post_upload(&self.http, upload, PAYLOAD.to_vec()).await;
        assert!(status.is_success(), "the exact bytes land: {status}");
    }

    pub async fn state(&self, reference: Uuid) -> (String, Option<String>) {
        sqlx::query_as("SELECT state, failed_reason FROM service_engine.blob WHERE id = $1")
            .bind(reference)
            .fetch_one(&self.pool)
            .await
            .expect("read the blob row")
    }

    pub async fn object_key(&self, reference: Uuid) -> String {
        sqlx::query_scalar("SELECT object_key FROM service_engine.blob WHERE id = $1")
            .bind(reference)
            .fetch_one(&self.pool)
            .await
            .expect("read the object key")
    }

    pub async fn probes(&self, reference: Uuid) -> Vec<serde_json::Value> {
        sqlx::query_scalar(
            "SELECT payload FROM service_engine.integration_outbox \
             WHERE subject LIKE $1 || '%' AND payload->'payload'->>'reference' = $2 \
             ORDER BY created_at",
        )
        .bind(PROBE_SUBJECT_PREFIX)
        .bind(reference.to_string())
        .fetch_all(&self.pool)
        .await
        .expect("read the probe events")
    }

    pub async fn all_probes(&self) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM service_engine.integration_outbox WHERE subject LIKE $1 || '%'",
        )
        .bind(PROBE_SUBJECT_PREFIX)
        .fetch_one(&self.pool)
        .await
        .expect("count the probe events")
    }

    pub async fn promote(
        &self,
        executor: &MutationExecutor<SamplePrincipal>,
        doc: Uuid,
    ) -> Result<BlobPromotion, service_engine::pipeline::MutationError> {
        executor
            .run::<PromoteDoc>(
                self.principal().await,
                PromoteDoc {
                    doc,
                    swallow_error: false,
                },
            )
            .await
    }

    pub async fn finish(self, running: Running) {
        running.stop().await;
        drop(self.nats);
        drop(self.minio);
        self.db.cleanup().await;
    }
}

pub struct Running {
    pub shutdown: std::sync::Arc<tokio::sync::Notify>,
    pub task: tokio::task::JoinHandle<Result<(), service_engine::error::EngineError>>,
    pub executor: MutationExecutor<SamplePrincipal>,
}

impl Running {
    pub async fn start(engine: Engine<SamplePrincipal>) -> Self {
        let readiness = engine.readiness();
        let shutdown = engine.shutdown_handle();
        let executor = engine.mutation_executor();
        let task = tokio::spawn(engine.run());
        await_ready(&readiness).await;
        Self {
            shutdown,
            task,
            executor,
        }
    }

    pub async fn stop(self) {
        self.shutdown.notify_one();
        self.task.await.expect("join").expect("run returns Ok");
    }
}
