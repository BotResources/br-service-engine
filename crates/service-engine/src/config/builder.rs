use std::net::SocketAddr;
use std::time::Duration;

use crate::blobs::BlobConfig;
use crate::config::EngineConfig;
use crate::graphql::MultipartConfig;

impl EngineConfig {
    pub fn with_service(mut self, service: impl Into<String>) -> Self {
        self.service = Some(service.into());
        self
    }

    pub fn with_service_version(mut self, service_version: impl Into<String>) -> Self {
        self.service_version = Some(service_version.into());
        self
    }

    pub fn with_schema_version_liveness(mut self, liveness: Duration) -> Self {
        self.schema_version_liveness = liveness;
        self
    }

    pub fn schema_service_version(&self) -> &str {
        self.service_version
            .as_deref()
            .unwrap_or(crate::schema::ENGINE_SCHEMA_VERSION)
    }

    pub fn with_blob_storage(mut self, blob: BlobConfig) -> Self {
        self.blob = Some(blob);
        self
    }

    pub fn with_multipart(mut self, multipart: MultipartConfig) -> Self {
        self.multipart = multipart;
        self
    }

    pub fn blob_service(&self) -> &str {
        self.service.as_deref().unwrap_or("blob")
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

    pub fn with_seal_retention(mut self, seal_retention: Duration) -> Self {
        self.seal_retention = seal_retention;
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

    pub fn with_offer_reconcile(mut self, offer_reconcile: Duration) -> Self {
        self.offer_reconcile = offer_reconcile;
        self
    }

    pub fn with_mirror_reconcile(mut self, mirror_reconcile: Duration) -> Self {
        self.mirror_reconcile = mirror_reconcile;
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

    pub fn with_publish_ack_timeout(mut self, publish_ack_timeout: Duration) -> Self {
        self.publish_ack_timeout = publish_ack_timeout;
        self
    }

    pub fn with_lock_timeout(mut self, lock_timeout: Duration) -> Self {
        self.lock_timeout = lock_timeout;
        self
    }

    pub fn with_ack_wait(mut self, ack_wait: Duration) -> Self {
        self.ack_wait = ack_wait;
        self
    }

    pub fn with_max_ack_pending(mut self, max_ack_pending: i64) -> Self {
        self.max_ack_pending = max_ack_pending;
        self
    }

    pub fn inbound_config(&self) -> crate::inbound::InboundConfig {
        crate::inbound::InboundConfig {
            ack_wait: self.ack_wait,
            max_ack_pending: self.max_ack_pending,
            ..crate::inbound::InboundConfig::default()
        }
    }

    pub fn with_listener_queue_threshold(mut self, listener_queue_threshold: f64) -> Self {
        self.listener_queue_threshold = listener_queue_threshold;
        self
    }

    pub fn with_listener_channel_capacity(mut self, listener_channel_capacity: usize) -> Self {
        self.listener_channel_capacity = listener_channel_capacity;
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

    pub fn with_message_retention(mut self, message_retention: Duration) -> Self {
        self.message_retention = message_retention;
        self
    }

    pub fn with_blob_reaper_interval(mut self, interval: Duration) -> Self {
        self.blob_reaper_interval = interval;
        self
    }

    pub fn with_http_addr(mut self, http_addr: SocketAddr) -> Self {
        self.http_addr = http_addr;
        self
    }

    pub fn with_migrate_connect_timeout(mut self, migrate_connect_timeout: Duration) -> Self {
        self.migrate_connect_timeout = migrate_connect_timeout;
        self
    }

    pub fn with_nats_url(mut self, nats_url: impl Into<String>) -> Self {
        self.nats_url = nats_url.into();
        self
    }

    pub fn with_app_role(mut self, app_role: impl Into<String>) -> Self {
        self.app_role = app_role.into();
        self
    }
}
