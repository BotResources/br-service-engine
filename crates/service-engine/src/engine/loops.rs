use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::error::EngineError;
use crate::housekeeping::beat::RepairRetry;
use crate::housekeeping::gc::SessionGc;
use crate::housekeeping::scheduled_message::{DEFAULT_SCHEDULED_MESSAGE_BATCH, fire_due};
use crate::nats::Nats;
use crate::principal::Principal;
use crate::runtime::SessionRuntime;
use crate::time::Timestamp;

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

pub(super) async fn run_scheduled_messages(
    pg: sqlx::PgPool,
    nats: Nats,
    interval: Duration,
    stop: Arc<Notify>,
) {
    let stopping = stop.notified();
    tokio::pin!(stopping);
    stopping.as_mut().enable();
    loop {
        if let Err(error) = fire_due(&pg, &nats, DEFAULT_SCHEDULED_MESSAGE_BATCH).await {
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

pub(super) async fn join_presence(task: Option<JoinHandle<()>>) {
    if let Some(task) = task {
        let _ = task.await;
    }
}
