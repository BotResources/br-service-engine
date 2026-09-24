mod connect;
mod counters;
mod drain;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tokio::sync::{Mutex, Notify};

use crate::accumulator::ChunkReader;
use crate::config::EngineConfig;
use crate::inbound::DeadLetters;
use crate::principal::Principal;
use crate::registry::RenderRegistry;
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
    pub(crate) lane: crate::lanes::LaneChannel,
    pub(crate) dead_letters: OnceLock<DeadLetters>,
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
        lanes: Vec<crate::lanes::Lane>,
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
            lane: crate::lanes::LaneChannel::new(lanes),
            dead_letters: OnceLock::new(),
        })
    }

    pub fn set_dead_letters(&self, dead_letters: DeadLetters) {
        let _ = self.dead_letters.set(dead_letters);
    }

    fn dead_letters(&self) -> DeadLetters {
        self.dead_letters
            .get()
            .cloned()
            .unwrap_or_else(|| DeadLetters::new(self.pg.clone()))
    }

    pub(crate) fn lane_channel(&self) -> crate::lanes::LaneChannel {
        self.lane.clone()
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn after_pass_signal(&self) -> Arc<Notify> {
        self.after_pass.clone()
    }

    pub async fn settle(&self, quiet: Duration, deadline: Duration) {
        let end = Instant::now() + deadline;
        loop {
            let tick = self.after_pass.notified();
            tokio::pin!(tick);
            tokio::select! {
                () = &mut tick => {}
                () = tokio::time::sleep(quiet) => return,
            }
            if Instant::now() >= end {
                return;
            }
        }
    }

    pub fn registry(&self) -> &RenderRegistry<P> {
        &self.registry
    }

    pub(crate) fn chunks(&self) -> &ChunkReader {
        &self.chunks
    }

    pub fn metrics(&self) -> RenderMetrics {
        self.counters.snapshot()
    }

    pub async fn live_sessions(&self) -> usize {
        self.table.lock().await.live_ids().len()
    }

    pub async fn pending_sessions(&self) -> usize {
        self.table.lock().await.pending_count()
    }

    pub async fn gc(&self) -> usize {
        let mut table = self.table.lock().await;
        table.reap_dropped(&self.dropped) + table.reap_expired(self.config.session_ttl)
    }
    pub fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    pub async fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        let mut table = self.table.lock().await;
        for session in table.iter_mut() {
            session.end();
        }
    }
}

mod passes;
