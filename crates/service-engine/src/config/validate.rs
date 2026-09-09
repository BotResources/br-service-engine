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
            ("listener_probe_timeout", self.listener_probe_timeout),
            ("session_max_age", self.session_max_age),
            ("lock_timeout", self.lock_timeout),
            ("nats_grace", self.nats_grace),
        ] {
            if value.is_zero() {
                return Err(EngineError::Config(format!("{label} must be non-zero")));
            }
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::name::{ChannelName, PodId};
    use std::time::Duration;

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
        assert_eq!(c.seal_retention, Duration::from_secs(86_400));
        assert_eq!(c.max_buffered_chunks, 10_000);
        assert_eq!(c.fold_cache_capacity, 10_000);
        assert_eq!(c.lease, Duration::from_secs(30));
        assert_eq!(c.offer_reconcile, Duration::from_secs(300));
        assert_eq!(c.listener_probe_timeout, Duration::from_secs(2));
        assert_eq!(c.repair_attempts, 5);
        assert_eq!(c.session_max_age, Duration::from_secs(43_200));
        assert_eq!(c.lock_timeout, Duration::from_secs(5));
        assert_eq!(c.listener_queue_threshold, 0.5);
        assert_eq!(c.listener_channel_capacity, 1_024);
        assert_eq!(c.nats_grace, Duration::from_secs(10));
        assert_eq!(c.window_capacity, 10_000);
        assert_eq!(c.impacts_per_commit, 1_000);
        assert_eq!(c.service, None);
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
        assert!(
            config()
                .with_lock_timeout(Duration::ZERO)
                .validate()
                .is_err()
        );
        assert!(config().with_nats_grace(Duration::ZERO).validate().is_err());
        assert!(
            config()
                .with_offer_reconcile(Duration::ZERO)
                .validate()
                .is_err()
        );
        assert!(config().with_window_capacity(0).validate().is_err());
        assert!(config().with_impacts_per_commit(0).validate().is_err());
        assert!(
            config()
                .with_listener_channel_capacity(0)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn a_session_max_age_that_is_not_above_the_idle_ttl_is_refused() {
        assert!(
            config()
                .with_session_max_age(Duration::from_secs(30))
                .validate()
                .is_err()
        );
        assert!(
            config()
                .with_session_ttl(Duration::from_secs(30))
                .with_session_max_age(Duration::from_secs(31))
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn a_listener_queue_threshold_outside_the_open_unit_interval_is_refused() {
        assert!(
            config()
                .with_listener_queue_threshold(0.0)
                .validate()
                .is_err()
        );
        assert!(
            config()
                .with_listener_queue_threshold(1.0)
                .validate()
                .is_ok()
        );
        assert!(
            config()
                .with_listener_queue_threshold(1.5)
                .validate()
                .is_err()
        );
        assert!(
            config()
                .with_listener_queue_threshold(f64::NAN)
                .validate()
                .is_err()
        );
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

    #[test]
    fn a_blank_service_label_is_refused_but_an_absent_one_is_accepted() {
        assert!(config().with_service("  ").validate().is_err());
        assert!(config().with_service("svc-chat").validate().is_ok());
        assert!(config().validate().is_ok());
    }
}
