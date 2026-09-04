use std::time::Duration;

use crate::error::EngineError;
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
    pub channel: ChannelName,
    pub pod_id: PodId,
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
            channel,
            pod_id,
        }
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

    pub fn validate(&self) -> Result<(), EngineError> {
        for (label, value) in [
            ("window", self.window),
            ("beat", self.beat),
            ("session_ttl", self.session_ttl),
            ("chunk_retention", self.chunk_retention),
            ("lease", self.lease),
            ("listener_probe_timeout", self.listener_probe_timeout),
        ] {
            if value.is_zero() {
                return Err(EngineError::Config(format!("{label} must be non-zero")));
            }
        }
        if self.session_buffer == 0 {
            return Err(EngineError::Config(
                "session_buffer must be non-zero".into(),
            ));
        }
        if self.max_buffered_chunks == 0 {
            return Err(EngineError::Config(
                "max_buffered_chunks must be non-zero".into(),
            ));
        }
        if self.fold_cache_capacity == 0 {
            return Err(EngineError::Config(
                "fold_cache_capacity must be non-zero".into(),
            ));
        }
        if self.reset_threshold == 0 {
            return Err(EngineError::Config(
                "reset_threshold must be non-zero".into(),
            ));
        }
        if self.max_held_impacts == 0 {
            return Err(EngineError::Config(
                "max_held_impacts must be non-zero".into(),
            ));
        }
        if self.repair_attempts == 0 {
            return Err(EngineError::Config(
                "repair_attempts must be non-zero, or a faulted session is ended before its first \
                 repair pass runs"
                    .into(),
            ));
        }
        if self.lease <= self.beat {
            return Err(EngineError::Config(
                "lease must outlast beat, otherwise a lease expires before its holder can renew it"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> EngineConfig {
        EngineConfig::new(
            ChannelName::new("service_engine_impact").unwrap(),
            PodId::new("svc-sample-0").unwrap(),
        )
    }

    #[test]
    fn a_fresh_config_carries_the_documented_defaults() {
        let c = config();
        assert_eq!(c.window, Duration::from_millis(100));
        assert_eq!(c.beat, Duration::from_secs(1));
        assert_eq!(c.session_ttl, Duration::from_secs(30));
        assert_eq!(c.session_buffer, 256);
        assert_eq!(c.reset_threshold, 200);
        assert_eq!(c.max_held_impacts, 1_024);
        assert_eq!(c.chunk_retention, Duration::from_secs(86_400));
        assert_eq!(c.max_buffered_chunks, 10_000);
        assert_eq!(c.fold_cache_capacity, 10_000);
        assert_eq!(c.lease, Duration::from_secs(30));
        assert_eq!(c.listener_probe_timeout, Duration::from_secs(2));
        assert_eq!(c.repair_attempts, 5);
        c.validate().unwrap();
    }

    #[test]
    fn a_zero_duration_or_zero_bound_is_refused() {
        assert!(config().with_window(Duration::ZERO).validate().is_err());
        assert!(config().with_session_buffer(0).validate().is_err());
        assert!(config().with_reset_threshold(0).validate().is_err());
        assert!(config().with_max_held_impacts(0).validate().is_err());
        assert!(config().with_max_buffered_chunks(0).validate().is_err());
        assert!(config().with_fold_cache_capacity(0).validate().is_err());
        assert!(config().with_repair_attempts(0).validate().is_err());
    }

    #[test]
    fn a_lease_that_does_not_outlast_the_beat_is_refused() {
        assert!(
            config()
                .with_lease(Duration::from_secs(1))
                .validate()
                .is_err()
        );
    }
}
