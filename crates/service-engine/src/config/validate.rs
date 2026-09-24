use crate::config::EngineConfig;
use crate::error::EngineError;

impl EngineConfig {
    pub fn validate(&self) -> Result<(), EngineError> {
        for (label, value) in [
            ("window", self.window),
            ("beat", self.beat),
            ("session_ttl", self.session_ttl),
            ("seal_retention", self.seal_retention),
            ("lease", self.lease),
            ("offer_reconcile", self.offer_reconcile),
            ("mirror_reconcile", self.mirror_reconcile),
            ("listener_probe_timeout", self.listener_probe_timeout),
            ("session_max_age", self.session_max_age),
            ("lock_timeout", self.lock_timeout),
            ("ack_wait", self.ack_wait),
            ("nats_grace", self.nats_grace),
            ("message_retention", self.message_retention),
            ("publish_ack_timeout", self.publish_ack_timeout),
            ("body_read_timeout", self.body_read_timeout),
        ] {
            if value.is_zero() {
                return Err(EngineError::Config(format!("{label} must be non-zero")));
            }
        }
        if self.publish_ack_timeout >= self.ack_wait {
            return Err(EngineError::Config(
                "publish_ack_timeout must stay below ack_wait, otherwise a single wedged JetStream \
                 publish can outlast the consumer's redelivery grace and stall the beat past it"
                    .into(),
            ));
        }
        if self.max_ack_pending <= 0 {
            return Err(EngineError::Config(
                "max_ack_pending must be positive, or the inbound consumer would let no frame be \
                 in flight"
                    .into(),
            ));
        }
        if self.lock_timeout >= self.ack_wait {
            return Err(EngineError::Config(
                "lock_timeout must stay below ack_wait, otherwise a pipeline transaction can still \
                 hold its row lock when the consumer redelivers the frame, so the redelivery races \
                 the in-flight effect instead of finding it done"
                    .into(),
            ));
        }
        for (label, value) in [
            ("session_buffer", self.session_buffer),
            ("max_buffered_chunks", self.max_buffered_chunks),
            ("fold_cache_capacity", self.fold_cache_capacity),
            ("reset_threshold", self.reset_threshold),
            ("max_held_impacts", self.max_held_impacts),
            ("window_capacity", self.window_capacity),
            ("impacts_per_commit", self.impacts_per_commit),
            ("listener_channel_capacity", self.listener_channel_capacity),
        ] {
            if value == 0 {
                return Err(EngineError::Config(format!("{label} must be non-zero")));
            }
        }
        if self.repair_attempts == 0 {
            return Err(EngineError::Config(
                "repair_attempts must be non-zero, or a faulted session is ended before its first \
                 repair pass runs"
                    .into(),
            ));
        }
        if !(self.listener_queue_threshold > 0.0 && self.listener_queue_threshold <= 1.0) {
            return Err(EngineError::Config(
                "listener_queue_threshold is a fraction of the notification queue and must lie in \
                 (0.0, 1.0]"
                    .into(),
            ));
        }
        if self.lease <= self.beat {
            return Err(EngineError::Config(
                "lease must outlast beat, otherwise a lease expires before its holder can renew it"
                    .into(),
            ));
        }
        if self.schema_version_liveness <= self.beat {
            return Err(EngineError::Config(
                "schema_version_liveness must outlast beat, otherwise a live pod's heartbeat lapses \
                 between two of its own beats and a pod of another version claims the store beside it"
                    .into(),
            ));
        }
        if self.session_max_age <= self.session_ttl {
            return Err(EngineError::Config(
                "session_max_age must outlast session_ttl, otherwise a live session is force-ended \
                 for a fresh passport sooner than an idle one is reaped"
                    .into(),
            ));
        }
        if let Some(service) = &self.service
            && service.trim().is_empty()
        {
            return Err(EngineError::Config(
                "service label, when set, must name the service so its metrics are attributable"
                    .into(),
            ));
        }
        self.multipart.validate()
    }
}
