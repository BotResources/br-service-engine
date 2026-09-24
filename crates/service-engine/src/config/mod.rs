mod body;
mod builder;
mod validate;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use crate::blobs::BlobConfig;
use crate::graphql::MultipartConfig;
use crate::inbound::{DEFAULT_ACK_WAIT, DEFAULT_MAX_ACK_PENDING};
use crate::name::{ChannelName, PodId};

pub const DEFAULT_WINDOW: Duration = Duration::from_millis(100);
pub const DEFAULT_BEAT: Duration = Duration::from_secs(1);
pub const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(30);
pub const DEFAULT_SESSION_BUFFER: usize = 256;
pub const DEFAULT_RESET_THRESHOLD: usize = 200;
pub const DEFAULT_MAX_HELD_IMPACTS: usize = 1_024;
pub const DEFAULT_SEAL_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);
pub const DEFAULT_MAX_BUFFERED_CHUNKS: usize = 10_000;
pub const DEFAULT_FOLD_CACHE_CAPACITY: usize = 10_000;
pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);
pub const DEFAULT_OFFER_RECONCILE: Duration = Duration::from_secs(300);
pub const DEFAULT_MIRROR_RECONCILE: Duration = Duration::from_secs(300);
pub const DEFAULT_LISTENER_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
pub const DEFAULT_REPAIR_ATTEMPTS: u32 = 5;
pub const DEFAULT_SESSION_MAX_AGE: Duration = Duration::from_secs(12 * 60 * 60);
pub const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_LISTENER_QUEUE_THRESHOLD: f64 = 0.5;
pub const DEFAULT_LISTENER_CHANNEL_CAPACITY: usize = 1_024;
pub const DEFAULT_NATS_GRACE: Duration = Duration::from_secs(10);
pub const DEFAULT_WINDOW_CAPACITY: usize = 10_000;
pub const DEFAULT_IMPACTS_PER_COMMIT: usize = 1_000;
pub const DEFAULT_MESSAGE_RETENTION: Duration = Duration::from_secs(60 * 60);
pub const DEFAULT_BLOB_REAPER_INTERVAL: Duration = Duration::from_secs(60);
pub const DEFAULT_SCHEMA_VERSION_LIVENESS: Duration = Duration::from_secs(30);
pub const DEFAULT_HTTP_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8080);
pub const DEFAULT_MIGRATE_CONNECT_TIMEOUT: Duration = Duration::from_secs(300);
pub const DEFAULT_MAX_BODY_BYTES: u64 = 16 * 1024 * 1024;
pub const DEFAULT_MULTIPART_MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;
pub const DEFAULT_MULTIPART_MAX_FILES: usize = 4;
pub const DEFAULT_BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EngineConfig {
    pub window: Duration,
    pub beat: Duration,
    pub session_ttl: Duration,
    pub session_buffer: usize,
    pub reset_threshold: usize,
    pub max_held_impacts: usize,
    pub seal_retention: Duration,
    pub max_buffered_chunks: usize,
    pub fold_cache_capacity: usize,
    pub lease: Duration,
    pub offer_reconcile: Duration,
    pub mirror_reconcile: Duration,
    pub listener_probe_timeout: Duration,
    pub repair_attempts: u32,
    pub session_max_age: Duration,
    pub lock_timeout: Duration,
    pub ack_wait: Duration,
    pub max_ack_pending: i64,
    pub listener_queue_threshold: f64,
    pub listener_channel_capacity: usize,
    pub nats_grace: Duration,
    pub window_capacity: usize,
    pub impacts_per_commit: usize,
    pub message_retention: Duration,
    pub publish_ack_timeout: Duration,
    pub blob_reaper_interval: Duration,
    pub schema_version_liveness: Duration,
    pub http_addr: SocketAddr,
    pub migrate_connect_timeout: Duration,
    pub channel: ChannelName,
    pub pod_id: PodId,
    pub nats_url: String,
    pub app_role: String,
    pub service: Option<String>,
    pub service_version: Option<String>,
    pub blob: Option<BlobConfig>,
    pub max_body_bytes: u64,
    pub multipart: MultipartConfig,
    pub body_read_timeout: Duration,
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
            seal_retention: DEFAULT_SEAL_RETENTION,
            max_buffered_chunks: DEFAULT_MAX_BUFFERED_CHUNKS,
            fold_cache_capacity: DEFAULT_FOLD_CACHE_CAPACITY,
            lease: DEFAULT_LEASE,
            offer_reconcile: DEFAULT_OFFER_RECONCILE,
            mirror_reconcile: DEFAULT_MIRROR_RECONCILE,
            listener_probe_timeout: DEFAULT_LISTENER_PROBE_TIMEOUT,
            repair_attempts: DEFAULT_REPAIR_ATTEMPTS,
            session_max_age: DEFAULT_SESSION_MAX_AGE,
            lock_timeout: DEFAULT_LOCK_TIMEOUT,
            ack_wait: DEFAULT_ACK_WAIT,
            max_ack_pending: DEFAULT_MAX_ACK_PENDING,
            listener_queue_threshold: DEFAULT_LISTENER_QUEUE_THRESHOLD,
            listener_channel_capacity: DEFAULT_LISTENER_CHANNEL_CAPACITY,
            nats_grace: DEFAULT_NATS_GRACE,
            window_capacity: DEFAULT_WINDOW_CAPACITY,
            impacts_per_commit: DEFAULT_IMPACTS_PER_COMMIT,
            message_retention: DEFAULT_MESSAGE_RETENTION,
            publish_ack_timeout: crate::nats::PUBLISH_ACK_TIMEOUT,
            blob_reaper_interval: DEFAULT_BLOB_REAPER_INTERVAL,
            schema_version_liveness: DEFAULT_SCHEMA_VERSION_LIVENESS,
            http_addr: DEFAULT_HTTP_ADDR,
            migrate_connect_timeout: DEFAULT_MIGRATE_CONNECT_TIMEOUT,
            channel,
            pod_id,
            nats_url: String::new(),
            app_role: String::new(),
            service: None,
            service_version: None,
            blob: None,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            multipart: MultipartConfig::default(),
            body_read_timeout: DEFAULT_BODY_READ_TIMEOUT,
        }
    }
}
