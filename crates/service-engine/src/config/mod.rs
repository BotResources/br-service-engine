mod validate;

use std::time::Duration;

use crate::name::{ChannelName, PodId};

pub const DEFAULT_WINDOW: Duration = Duration::from_millis(100);
pub const DEFAULT_BEAT: Duration = Duration::from_secs(1);
pub const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(30);
pub const DEFAULT_SESSION_BUFFER: usize = 256;
pub const DEFAULT_RESET_THRESHOLD: usize = 200;
pub const DEFAULT_MAX_HELD_IMPACTS: usize = 1_024;
pub const DEFAULT_CHUNK_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
pub const DEFAULT_MAX_BUFFERED_CHUNKS: usize = 10_000;
pub const DEFAULT_FOLD_CACHE_CAPACITY: usize = 10_000;
pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);
pub const DEFAULT_LISTENER_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
pub const DEFAULT_REPAIR_ATTEMPTS: u32 = 5;
pub const DEFAULT_SESSION_MAX_AGE: Duration = Duration::from_secs(12 * 60 * 60);
pub const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_LISTENER_QUEUE_THRESHOLD: f64 = 0.5;
pub const DEFAULT_NATS_GRACE: Duration = Duration::from_secs(10);
pub const DEFAULT_WINDOW_CAPACITY: usize = 10_000;
pub const DEFAULT_IMPACTS_PER_COMMIT: usize = 1_000;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EngineConfig {
    pub window: Duration,
    pub beat: Duration,
    pub session_ttl: Duration,
    pub session_buffer: usize,
    pub reset_threshold: usize,
    pub max_held_impacts: usize,
    pub chunk_retention: Duration,
    pub max_buffered_chunks: usize,
    pub fold_cache_capacity: usize,
    pub lease: Duration,
    pub listener_probe_timeout: Duration,
    pub repair_attempts: u32,
    pub session_max_age: Duration,
    pub lock_timeout: Duration,
    pub listener_queue_threshold: f64,
    pub nats_grace: Duration,
    pub window_capacity: usize,
    pub impacts_per_commit: usize,
    pub channel: ChannelName,
    pub pod_id: PodId,
    pub service: Option<String>,
}

impl EngineConfig {
    pub fn new(channel: ChannelName, pod_id: PodId) -> Self {
        Self {
            window: DEFAULT_WINDOW,
            beat: DEFAULT_BEAT,
            session_ttl: DEFAULT_SESSION_TTL,
            session_buffer: DEFAULT_SESSION_BUFFER,
            reset_threshold: DEFAULT_RESET_THRESHOLD,
            max_held_impacts: DEFAULT_MAX_HELD_IMPACTS,
            chunk_retention: DEFAULT_CHUNK_RETENTION,
            max_buffered_chunks: DEFAULT_MAX_BUFFERED_CHUNKS,
            fold_cache_capacity: DEFAULT_FOLD_CACHE_CAPACITY,
            lease: DEFAULT_LEASE,
            listener_probe_timeout: DEFAULT_LISTENER_PROBE_TIMEOUT,
            repair_attempts: DEFAULT_REPAIR_ATTEMPTS,
            session_max_age: DEFAULT_SESSION_MAX_AGE,
            lock_timeout: DEFAULT_LOCK_TIMEOUT,
            listener_queue_threshold: DEFAULT_LISTENER_QUEUE_THRESHOLD,
            nats_grace: DEFAULT_NATS_GRACE,
            window_capacity: DEFAULT_WINDOW_CAPACITY,
            impacts_per_commit: DEFAULT_IMPACTS_PER_COMMIT,
            channel,
            pod_id,
            service: None,
        }
    }

    pub fn with_service(mut self, service: impl Into<String>) -> Self {
        self.service = Some(service.into());
        self
    }

    pub fn ephemeral_bucket(&self) -> Option<String> {
        self.service
            .as_ref()
            .map(|service| format!("EPHEMERAL_{service}"))
    }

    pub fn with_window(mut self, window: Duration) -> Self {
        self.window = window;
        self
    }

    pub fn with_beat(mut self, beat: Duration) -> Self {
        self.beat = beat;
        self
    }

    pub fn with_session_ttl(mut self, session_ttl: Duration) -> Self {
        self.session_ttl = session_ttl;
        self
    }

    pub fn with_session_buffer(mut self, session_buffer: usize) -> Self {
        self.session_buffer = session_buffer;
        self
    }

    pub fn with_reset_threshold(mut self, reset_threshold: usize) -> Self {
        self.reset_threshold = reset_threshold;
        self
    }

    pub fn with_max_held_impacts(mut self, max_held_impacts: usize) -> Self {
        self.max_held_impacts = max_held_impacts;
        self
    }

    pub fn with_chunk_retention(mut self, chunk_retention: Duration) -> Self {
        self.chunk_retention = chunk_retention;
        self
    }

    pub fn with_max_buffered_chunks(mut self, max_buffered_chunks: usize) -> Self {
        self.max_buffered_chunks = max_buffered_chunks;
        self
    }

    pub fn with_fold_cache_capacity(mut self, fold_cache_capacity: usize) -> Self {
        self.fold_cache_capacity = fold_cache_capacity;
        self
    }

    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    pub fn with_listener_probe_timeout(mut self, listener_probe_timeout: Duration) -> Self {
        self.listener_probe_timeout = listener_probe_timeout;
        self
    }

    pub fn with_repair_attempts(mut self, repair_attempts: u32) -> Self {
        self.repair_attempts = repair_attempts;
        self
    }

    pub fn with_session_max_age(mut self, session_max_age: Duration) -> Self {
        self.session_max_age = session_max_age;
        self
    }

    pub fn with_lock_timeout(mut self, lock_timeout: Duration) -> Self {
        self.lock_timeout = lock_timeout;
        self
    }

    pub fn with_listener_queue_threshold(mut self, listener_queue_threshold: f64) -> Self {
        self.listener_queue_threshold = listener_queue_threshold;
        self
    }

    pub fn with_nats_grace(mut self, nats_grace: Duration) -> Self {
        self.nats_grace = nats_grace;
        self
    }

    pub fn with_window_capacity(mut self, window_capacity: usize) -> Self {
        self.window_capacity = window_capacity;
        self
    }

    pub fn with_impacts_per_commit(mut self, impacts_per_commit: usize) -> Self {
        self.impacts_per_commit = impacts_per_commit;
        self
    }
}
