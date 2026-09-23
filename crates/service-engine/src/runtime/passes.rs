use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use futures_util::stream::BoxStream;

use crate::error::{EngineError, TransportError};
use crate::impact::{Impact, TransportEvent};
use crate::principal::Principal;
use crate::render::pass::{PassContext, PassReport};
use crate::runtime::SessionRuntime;
use crate::stop::Stop;

impl<P: Principal> SessionRuntime<P> {
    pub async fn retry_repairs(&self) -> Result<usize, EngineError> {
        let started = Instant::now();
        let mut table = self.table.lock().await;
        table.reap_dropped(&self.dropped);
        {
            let aged = table.reap_aged(self.config.session_max_age);
            if aged > 0 {
                crate::observe::record_sessions_ended(aged, crate::observe::REASON_MAX_AGE);
            }
        }
        if !table
            .iter()
            .any(|session| session.is_live() && session.repair_pending)
        {
            return Ok(0);
        }
        let dead_letters = self.dead_letters();
        let ctx = PassContext {
            pg: &self.pg,
            registry: &self.registry,
            chunks: &self.chunks,
            config: &self.config,
            dead_letters: Some(&dead_letters),
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
        let dead_letters = self.dead_letters();
        let ctx = PassContext {
            pg: &self.pg,
            registry: &self.registry,
            chunks: &self.chunks,
            config: &self.config,
            dead_letters: Some(&dead_letters),
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
    pub async fn run(
        self: Arc<Self>,
        source: BoxStream<'static, Result<TransportEvent, TransportError>>,
        shutdown: Arc<Stop>,
    ) {
        let (sink, receiver) = tokio::sync::mpsc::channel(self.config.listener_channel_capacity);
        let drain_stop = crate::stop::Stop::new();
        let listener = tokio::spawn(super::drain::drain_listener(
            source,
            sink,
            drain_stop.clone(),
        ));
        let mut events = super::drain::receiver_stream(receiver);
        let stopping = shutdown.stopped();
        tokio::pin!(stopping);
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
        drain_stop.stop();
        let _ = listener.await;
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
                tracing::warn!(error = %crate::chain::describe(&error), "the impact transport reported an incident");
                None
            }
            Ok(TransportEvent::Reconnected) => {
                self.counters
                    .transport_reconnects
                    .fetch_add(1, Ordering::Relaxed);
                crate::observe::record_reconnect();
                if let Err(error) = self.resnapshot_all().await {
                    tracing::error!(error = %crate::chain::describe(&error), "resetting every session after a reconnect failed");
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
            tracing::error!(error = %crate::chain::describe(&error), "a render pass failed");
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
                tracing::error!(error = %crate::chain::describe(&error), "a coalesced render pass failed");
            }
            self.signal_after_pass();
        }
    }
}
