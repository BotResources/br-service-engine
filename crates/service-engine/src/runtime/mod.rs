mod connect;
mod counters;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use sqlx::PgPool;
use tokio::sync::{Mutex, Notify};

use crate::accumulator::ChunkReader;
use crate::config::EngineConfig;
use crate::error::{EngineError, TransportError};
use crate::impact::{Impact, TransportEvent};
use crate::principal::Principal;
use crate::registry::RenderRegistry;
use crate::render::pass::{PassContext, PassReport};
use crate::session::store::SessionTable;
use crate::session::stream::DropList;

pub use counters::RenderMetrics;

use counters::Counters;

pub struct SessionRuntime<P: Principal> {
    pub(crate) config: EngineConfig,
    pub(crate) pg: PgPool,
    pub(crate) registry: Arc<RenderRegistry<P>>,
    pub(crate) chunks: ChunkReader,
    pub(crate) table: Mutex<SessionTable<P>>,
    pub(crate) dropped: DropList,
    pub(crate) counters: Counters,
    pub(crate) shutting_down: AtomicBool,
    pub(crate) after_pass: Arc<Notify>,
}

impl<P: Principal> std::fmt::Debug for SessionRuntime<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionRuntime")
            .field("pod", &self.config.pod_id)
            .field("registry", &self.registry)
            .finish_non_exhaustive()
    }
}

impl<P: Principal> SessionRuntime<P> {
    pub fn new(
        config: EngineConfig,
        pg: PgPool,
        registry: RenderRegistry<P>,
        chunks: ChunkReader,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            pg,
            registry: Arc::new(registry),
            chunks,
            table: Mutex::new(SessionTable::new()),
            dropped: DropList::default(),
            counters: Counters::default(),
            shutting_down: AtomicBool::new(false),
            after_pass: Arc::new(Notify::new()),
        })
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn after_pass_signal(&self) -> Arc<Notify> {
        self.after_pass.clone()
    }

    pub fn registry(&self) -> &RenderRegistry<P> {
        &self.registry
    }

    pub fn metrics(&self) -> RenderMetrics {
        self.counters.snapshot()
    }

    pub async fn live_sessions(&self) -> usize {
        self.table.lock().await.live_ids().len()
    }

    pub async fn gc(&self) -> usize {
        let mut table = self.table.lock().await;
        table.reap_dropped(&self.dropped) + table.reap_expired(self.config.session_ttl)
    }

    pub async fn retry_repairs(&self) -> Result<usize, EngineError> {
        let started = Instant::now();
        let mut table = self.table.lock().await;
        table.reap_dropped(&self.dropped);
        if !table
            .iter()
            .any(|session| session.is_live() && session.repair_pending)
        {
            return Ok(0);
        }
        let ctx = PassContext {
            pg: &self.pg,
            registry: &self.registry,
            chunks: &self.chunks,
            config: &self.config,
        };
        let report = crate::render::pass::run_pass_focused(&ctx, &mut table, &[], None).await?;
        let live = table.live_ids().len();
        let pending = table.pending_count();
        drop(table);
        self.counters.absorb(&report);
        crate::observe::record_pass(&report, started.elapsed().as_secs_f64(), live, pending);
        Ok(report.repaired() + report.ended)
    }

    pub async fn render(&self, impacts: Vec<Impact>) -> Result<PassReport, EngineError> {
        self.render_focused(impacts, None).await
    }

    pub(crate) async fn render_session(
        &self,
        id: crate::session::SessionId,
        impacts: Vec<Impact>,
    ) -> Result<PassReport, EngineError> {
        self.render_focused(impacts, Some(id)).await
    }

    async fn render_focused(
        &self,
        impacts: Vec<Impact>,
        focus: Option<crate::session::SessionId>,
    ) -> Result<PassReport, EngineError> {
        let started = Instant::now();
        let mut table = self.table.lock().await;
        table.reap_dropped(&self.dropped);
        let ctx = PassContext {
            pg: &self.pg,
            registry: &self.registry,
            chunks: &self.chunks,
            config: &self.config,
        };
        let report =
            crate::render::pass::run_pass_focused(&ctx, &mut table, &impacts, focus).await?;
        let live = table.live_ids().len();
        let pending = table.pending_count();
        drop(table);
        self.counters.absorb(&report);
        let elapsed = started.elapsed();
        if elapsed > self.config.window {
            self.counters.overflows.fetch_add(1, Ordering::Relaxed);
            crate::observe::record_overflow();
        }
        crate::observe::record_pass(&report, elapsed.as_secs_f64(), live, pending);
        Ok(report)
    }

    pub async fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        let mut table = self.table.lock().await;
        for session in table.iter_mut() {
            session.end();
        }
    }

    pub async fn run(
        self: Arc<Self>,
        mut events: BoxStream<'static, Result<TransportEvent, TransportError>>,
        shutdown: Arc<Notify>,
    ) {
        let stopping = shutdown.notified();
        tokio::pin!(stopping);
        stopping.as_mut().enable();
        loop {
            let event = tokio::select! {
                () = &mut stopping => break,
                event = events.next() => event,
            };
            let Some(event) = event else { break };
            match self.absorb(event).await {
                Some(impacts) => {
                    self.pass_and_coalesce(impacts, &mut events).await;
                }
                None => continue,
            }
        }
        self.shutdown().await;
    }

    fn signal_after_pass(&self) {
        self.after_pass.notify_one();
    }

    async fn absorb(&self, event: Result<TransportEvent, TransportError>) -> Option<Vec<Impact>> {
        match event {
            Err(error) => {
                self.counters
                    .transport_incidents
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(%error, "the impact transport reported an incident");
                None
            }
            Ok(TransportEvent::Reconnected) => {
                self.counters
                    .transport_reconnects
                    .fetch_add(1, Ordering::Relaxed);
                crate::observe::record_reconnect();
                if let Err(error) = self.resnapshot_all().await {
                    tracing::error!(%error, "resetting every session after a reconnect failed");
                }
                None
            }
            Ok(TransportEvent::Impacts(impacts)) => Some(impacts),
        }
    }

    async fn pass_and_coalesce(
        &self,
        leading: Vec<Impact>,
        events: &mut BoxStream<'static, Result<TransportEvent, TransportError>>,
    ) {
        if let Err(error) = self.render(leading).await {
            tracing::error!(%error, "a render pass failed");
        }
        self.signal_after_pass();
        let deadline = Instant::now() + self.config.window;
        let mut coalesced: Vec<Impact> = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining == Duration::ZERO {
                break;
            }
            match tokio::time::timeout(remaining, events.next()).await {
                Err(_) => break,
                Ok(None) => break,
                Ok(Some(event)) => {
                    if let Some(impacts) = self.absorb(event).await {
                        coalesced.extend(impacts);
                    }
                }
            }
        }
        if !coalesced.is_empty() {
            if let Err(error) = self.render(coalesced).await {
                tracing::error!(%error, "a coalesced render pass failed");
            }
            self.signal_after_pass();
        }
    }
}
