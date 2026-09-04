use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use service_engine::accumulator::AccumulatorRuntime;
use service_engine::config::EngineConfig;
use service_engine::delta::{Delta, ErasedView};
use service_engine::error::TransportError;
use service_engine::impact::{Dims, Impact, TransportEvent};
use service_engine::name::{ChannelName, PodId, ProjectorName};
use service_engine::registry::RenderRegistry;
use service_engine::runtime::SessionRuntime;
use service_engine::session::{AttachRequest, SessionStream, WindowParams, WindowSpec};
use sqlx::PgPool;
use uuid::Uuid;

use service_engine::wire::Cause;

use crate::sample::assignment::Assignment;
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver, SampleRls};
use crate::sample::transport::RecordingTransport;

pub const CHUNK_RETENTION: Duration = Duration::from_secs(3600);

pub fn render_config(pod: &str) -> EngineConfig {
    EngineConfig::new(
        ChannelName::new("sample_render").expect("a valid channel"),
        PodId::new(pod).expect("a valid pod id"),
    )
}

pub fn registry() -> RenderRegistry<SamplePrincipal> {
    let mut registry = RenderRegistry::new();
    registry.bind_noun::<crate::sample::assignment::Assignment>();
    registry.bind_noun::<crate::sample::note::Note>();
    registry.register_rls(SampleRls);
    registry.register_principal_resolver(SamplePrincipalResolver);
    registry
}

pub fn runtime(
    pool: &PgPool,
    config: EngineConfig,
    registry: RenderRegistry<SamplePrincipal>,
) -> Arc<SessionRuntime<SamplePrincipal>> {
    let accumulators =
        AccumulatorRuntime::new(pool.clone(), Arc::new(RecordingTransport), CHUNK_RETENTION);
    SessionRuntime::new(
        config,
        pool.clone(),
        registry,
        accumulators.reader().clone(),
    )
}

pub fn window(projector: ProjectorName, rls: bool) -> WindowSpec {
    WindowSpec::new(projector, WindowParams::none(), rls)
}

pub fn attach_request(
    principal: &SamplePrincipal,
    windows: Vec<WindowSpec>,
) -> AttachRequest<SamplePrincipal> {
    AttachRequest::new(principal.clone(), windows)
}

pub async fn member(pool: &PgPool, user: Uuid, tenant: Uuid) -> SamplePrincipal {
    sqlx::query(
        "INSERT INTO sample_member (user_id, tenant_id) VALUES ($1, $2) \
         ON CONFLICT (user_id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id",
    )
    .bind(user)
    .bind(tenant)
    .execute(pool)
    .await
    .expect("register the sample member");
    SamplePrincipal::new(user, tenant)
}

pub async fn move_member(pool: &PgPool, user: Uuid, tenant: Uuid) {
    sqlx::query("UPDATE sample_member SET tenant_id = $1 WHERE user_id = $2")
        .bind(tenant)
        .bind(user)
        .execute(pool)
        .await
        .expect("move the sample member");
}

pub async fn forget_member(pool: &PgPool, user: Uuid) {
    sqlx::query("DELETE FROM sample_member WHERE user_id = $1")
        .bind(user)
        .execute(pool)
        .await
        .expect("forget the sample member");
}

pub async fn assignment(pool: &PgPool, tenant: Uuid, title: &str) -> Uuid {
    let id = Uuid::now_v7();
    sqlx::query("INSERT INTO sample_assignment (id, tenant_id, title) VALUES ($1, $2, $3)")
        .bind(id)
        .bind(tenant)
        .bind(title)
        .execute(pool)
        .await
        .expect("insert the sample assignment");
    id
}

pub async fn delete_assignment(pool: &PgPool, id: Uuid) {
    sqlx::query("DELETE FROM sample_assignment WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .expect("delete the sample assignment");
}

pub async fn retitle(pool: &PgPool, id: Uuid, title: &str) {
    sqlx::query("UPDATE sample_assignment SET title = $1 WHERE id = $2")
        .bind(title)
        .bind(id)
        .execute(pool)
        .await
        .expect("retitle the sample assignment");
}

pub async fn reassign(pool: &PgPool, id: Uuid, tenant: Uuid) {
    sqlx::query("UPDATE sample_assignment SET tenant_id = $1 WHERE id = $2")
        .bind(tenant)
        .bind(id)
        .execute(pool)
        .await
        .expect("move the sample assignment to another tenant");
}

pub async fn note(pool: &PgPool, assignment_id: Uuid, seq: i32, body: &str) {
    sqlx::query("INSERT INTO sample_note (assignment_id, seq, body) VALUES ($1, $2, $3)")
        .bind(assignment_id)
        .bind(seq)
        .bind(body)
        .execute(pool)
        .await
        .expect("insert the sample note");
}

pub async fn drain(stream: &mut SessionStream) -> Vec<Delta> {
    let mut drained = Vec::new();
    while let Ok(Some(delta)) = tokio::time::timeout(Duration::from_millis(50), stream.next()).await
    {
        drained.push(delta);
    }
    drained
}

pub async fn next_delta(stream: &mut SessionStream, within: Duration) -> Option<Delta> {
    tokio::time::timeout(within, stream.next())
        .await
        .unwrap_or_default()
}

pub struct ImpactFeed {
    sender: tokio::sync::mpsc::UnboundedSender<Result<TransportEvent, TransportError>>,
}

impl ImpactFeed {
    pub fn new() -> (
        Self,
        BoxStream<'static, Result<TransportEvent, TransportError>>,
    ) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let stream = futures_util::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|event| (event, receiver))
        })
        .boxed();
        (Self { sender }, stream)
    }

    pub fn impacts(&self, impacts: Vec<Impact>) {
        let _ = self.sender.send(Ok(TransportEvent::Impacts(impacts)));
    }

    pub fn reconnected(&self) {
        let _ = self.sender.send(Ok(TransportEvent::Reconnected));
    }
}

pub fn resource(id: &Uuid, dims: Dims) -> Impact {
    Impact::resource::<Assignment>(id, dims).expect("an assignment key encodes")
}

pub fn caused(id: &Uuid, dims: Dims, cause: &str) -> Impact {
    Impact::resource_caused::<Assignment>(id, dims, Cause::encode(&cause).expect("a cause encodes"))
        .expect("an assignment key encodes")
}

pub fn reset_views(delta: &Delta) -> &[ErasedView] {
    match delta {
        Delta::Reset { views, .. } => views,
        other => panic!("the first frame of a session must be a Reset, got {other:?}"),
    }
}

pub fn upserted(delta: &Delta) -> &ErasedView {
    match delta {
        Delta::Upsert { view, .. } => view,
        other => panic!("expected an Upsert, got {other:?}"),
    }
}

pub fn upsert_cause(delta: &Delta) -> Option<String> {
    match delta {
        Delta::Upsert { cause, .. } => cause
            .as_ref()
            .map(|cause| cause.decode::<String>().expect("a cause decodes")),
        other => panic!("expected an Upsert, got {other:?}"),
    }
}

pub fn removed_key(delta: &Delta) -> Uuid {
    match delta {
        Delta::Remove { key, .. } => key.decode::<Uuid>().expect("the removed key decodes"),
        other => panic!("expected a Remove, got {other:?}"),
    }
}

pub fn assignment_ids(views: &[ErasedView]) -> Vec<Uuid> {
    views
        .iter()
        .map(|view| {
            view.key
                .decode::<Uuid>()
                .expect("an assignment key decodes")
        })
        .collect()
}
