use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use tokio::task::JoinHandle;

use crate::config::EngineConfig;
use crate::error::{EngineError, TransportError};
use crate::housekeeping::beat::RepairRetry;
use crate::housekeeping::gc::SessionGc;
use crate::housekeeping::lane::ResetAll;
use crate::housekeeping::scheduled_message::{
    DEFAULT_SCHEDULED_MESSAGE_BATCH, DEFAULT_SCHEDULED_MESSAGE_BUDGET, fire_due,
};
use crate::impact::TransportEvent;
use crate::inbound::DeadLetters;
use crate::nats::Nats;
use crate::presence::PresenceRegistry;
use crate::principal::Principal;
use crate::runtime::SessionRuntime;
use crate::stop::Stop;
use crate::time::Timestamp;
use crate::transport::{ImpactTransport, PgListenNotify};

pub(super) struct RenderGc<P: Principal>(pub(super) Arc<SessionRuntime<P>>);

impl<P: Principal> SessionGc for RenderGc<P> {
    fn collect<'a>(&'a self, _now: Timestamp) -> BoxFuture<'a, Result<usize, EngineError>> {
        Box::pin(async move { Ok(self.0.gc().await) })
    }
}

pub(super) struct RenderRepairs<P: Principal>(pub(super) Arc<SessionRuntime<P>>);

impl<P: Principal> RepairRetry for RenderRepairs<P> {
    fn retry<'a>(&'a self) -> BoxFuture<'a, Result<usize, EngineError>> {
        Box::pin(async move { self.0.retry_repairs().await })
    }
}

pub(super) struct RenderReset<P: Principal>(pub(super) Arc<SessionRuntime<P>>);

impl<P: Principal> ResetAll for RenderReset<P> {
    fn reset_all(&self) -> BoxFuture<'_, Result<usize, EngineError>> {
        Box::pin(async move { self.0.resnapshot_all().await })
    }
}

pub(super) async fn run_scheduled_messages(
    pg: sqlx::PgPool,
    nats: Nats,
    dead_letters: DeadLetters,
    interval: Duration,
    stop: Arc<Stop>,
) {
    let stopping = stop.stopped();
    tokio::pin!(stopping);
    loop {
        if let Err(error) = fire_due(
            &pg,
            &nats,
            &dead_letters,
            DEFAULT_SCHEDULED_MESSAGE_BATCH,
            DEFAULT_SCHEDULED_MESSAGE_BUDGET,
        )
        .await
        {
            tracing::warn!(
                reason = %crate::chain::describe(&error),
                "the beat could not fire the scheduled messages whose time has passed",
            );
        }
        tokio::select! {
            biased;
            () = &mut stopping => return,
            () = tokio::time::sleep(interval) => {}
        }
    }
}

pub(super) async fn open_presence_stream<P: Principal>(
    nats: &Nats,
    config: &EngineConfig,
    presence: &PresenceRegistry<P>,
    transport: &Arc<PgListenNotify>,
    stop_presence: Arc<Stop>,
) -> Result<
    (
        BoxStream<'static, Result<TransportEvent, TransportError>>,
        Option<JoinHandle<()>>,
    ),
    EngineError,
> {
    if presence.is_empty() {
        return Ok((transport.listen(), None));
    }
    let Some(bucket_name) = config.ephemeral_bucket() else {
        return Err(EngineError::Config(
            "a presence lane is registered but no service is configured, so the \
             EPHEMERAL_{service} bucket has no name; call EngineConfig::with_service"
                .into(),
        ));
    };
    let lanes = presence.lanes();
    let bucket = crate::presence::bind_and_seed(nats, &bucket_name, &lanes).await?;
    let _ = presence.bucket_slot().set(bucket.clone());
    let (sender, stream) = crate::presence::presence_channel();
    let presence_task = tokio::spawn(crate::presence::run_watch(
        nats.clone(),
        bucket,
        lanes,
        sender,
        stop_presence,
    ));
    Ok((
        futures_util::stream::select(transport.listen(), stream).boxed(),
        Some(presence_task),
    ))
}

pub(super) async fn join_presence(task: Option<JoinHandle<()>>) {
    if let Some(task) = task {
        let _ = task.await;
    }
}
