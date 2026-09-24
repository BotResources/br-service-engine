use std::sync::Arc;
use std::time::Duration;

use async_graphql::{Context, EmptySubscription, Object, Result, Upload};
use service_engine::graphql::{
    MultipartConfig, OrInternal, RootPrefix, SliceFragment, coded_error,
};
use service_engine::nats::Nats;
use service_engine::{Engine, EngineConfig, Readiness, ReadinessHandle, engine_schema};
use tokio::io::AsyncReadExt;

use crate::infra::TestDb;
use crate::sample::graphql::boot::{GraphqlService, base_config, free_loopback_addr};
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver};

pub const UPLOAD_MISMATCH_CODE: &str = "UPLOAD_MISMATCH";

pub fn upload_digest(filename: &str, content: &[u8]) -> String {
    let hash = content
        .iter()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        });
    format!("{filename}:{}:{hash:016x}", content.len())
}

#[derive(Default)]
pub struct UploadQueryRoot;

#[Object]
impl UploadQueryRoot {
    async fn sample_upload_ready(&self) -> bool {
        true
    }
}

#[derive(Default)]
pub struct UploadMutationRoot;

#[Object]
impl UploadMutationRoot {
    async fn sample_upload_verify(
        &self,
        ctx: &Context<'_>,
        files: Vec<Upload>,
        expected: Vec<String>,
    ) -> Result<bool> {
        let mut read = Vec::with_capacity(files.len());
        for file in &files {
            let upload = file.value(ctx).or_internal("open a spooled upload")?;
            let mut content = Vec::new();
            tokio::fs::File::from_std(upload.content)
                .read_to_end(&mut content)
                .await
                .or_internal("read a spooled upload")?;
            read.push(upload_digest(&upload.filename, &content));
        }
        if read == expected {
            Ok(true)
        } else {
            Err(coded_error(UPLOAD_MISMATCH_CODE, format!("read {read:?}")))
        }
    }
}

fn upload_slice() -> SliceFragment {
    SliceFragment::from_claims(
        "upload",
        vec![
            "sampleUploadReady".to_string(),
            "sampleUploadVerify".to_string(),
        ],
        Vec::new(),
    )
}

pub async fn boot_upload_service(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    multipart: MultipartConfig,
) -> GraphqlService {
    boot_upload_service_with(db, nats, channel, pod, |config| {
        config.with_multipart(multipart)
    })
    .await
}

pub async fn boot_upload_service_with(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    configure: impl FnOnce(EngineConfig) -> EngineConfig,
) -> GraphqlService {
    let addr = free_loopback_addr().await;
    let config = configure(base_config(channel, pod).with_http_addr(addr));

    let mut engine = Engine::<SamplePrincipal>::boot(
        config,
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the upload engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .declare_root_prefix(
            RootPrefix::from_snake("sample").expect("`sample` is a valid root prefix"),
        )
        .expect("declare the sample root prefix");
    engine
        .register_schema_slice(upload_slice())
        .expect("the upload slice owns its root field");

    let readiness = engine.readiness();
    let engine_stop = engine.shutdown_handle();
    let state = Arc::new(engine.graphql_state());
    let schema = engine_schema(
        UploadQueryRoot,
        UploadMutationRoot,
        EmptySubscription,
        state.clone(),
    );
    engine.set_schema_sdl(schema.sdl());
    let app = service_engine::app(schema, state, readiness.clone());

    let handle = tokio::spawn(engine.run_with(app));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    while readiness.snapshot() != Readiness::Ready {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the upload engine never reached readiness UP"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    GraphqlService {
        base_url: format!("http://{addr}"),
        engine_stop,
        handle,
    }
}
