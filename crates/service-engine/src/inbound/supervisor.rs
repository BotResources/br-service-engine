use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::housekeeping::backoff::Backoff;

pub const DEFAULT_HEALTHY_UPTIME: Duration = Duration::from_secs(5);
pub const DEFAULT_FAILURE_THRESHOLD: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ServeExit {
    Cancelled,
    Ended,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SupervisorConfig {
    pub healthy_uptime: Duration,
    pub failure_threshold: u32,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            healthy_uptime: DEFAULT_HEALTHY_UPTIME,
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
        }
    }
}

struct Inner {
    tx: watch::Sender<bool>,
    unhealthy: Mutex<BTreeSet<String>>,
}

#[derive(Clone)]
pub struct InboundHealth {
    inner: Arc<Inner>,
}

impl InboundHealth {
    pub fn new() -> (Self, watch::Receiver<bool>) {
        let (tx, rx) = watch::channel(true);
        (
            Self {
                inner: Arc::new(Inner {
                    tx,
                    unhealthy: Mutex::new(BTreeSet::new()),
                }),
            },
            rx,
        )
    }

    fn report(&self, name: &str, healthy: bool) {
        let up = {
            let mut set = self
                .inner
                .unhealthy
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if healthy {
                set.remove(name);
            } else {
                set.insert(name.to_string());
            }
            set.is_empty()
        };
        self.inner.tx.send_if_modified(|current| {
            if *current == up {
                false
            } else {
                *current = up;
                true
            }
        });
    }
}

pub(crate) struct HealthTracker {
    name: String,
    health: InboundHealth,
    failures: u32,
    threshold: u32,
    backoff: Backoff,
}

impl HealthTracker {
    pub(crate) fn new(name: String, health: InboundHealth, threshold: u32) -> Self {
        Self {
            name,
            health,
            failures: 0,
            threshold,
            backoff: Backoff::default(),
        }
    }

    pub(crate) fn connected(&mut self) {
        if self.failures < self.threshold {
            self.health.report(&self.name, true);
        }
    }

    pub(crate) fn promote(&mut self) {
        self.failures = 0;
        self.backoff.succeed();
        self.health.report(&self.name, true);
    }

    pub(crate) fn failed(&mut self, ran_for: Duration, uptime: Duration, reason: &str) -> Duration {
        if ran_for >= uptime {
            self.failures = 0;
            self.backoff.succeed();
        }
        self.failures = self.failures.saturating_add(1);
        let healthy = self.failures < self.threshold;
        self.health.report(&self.name, healthy);
        let delay = self.backoff.fail(Instant::now());
        tracing::warn!(
            supervised = %self.name,
            attempts = self.failures,
            healthy,
            retry_in_ms = delay.as_millis(),
            reason,
            "a supervised source stopped; it restarts with bounded backoff and, past the failure \
             threshold, lowers readiness so a dead loop never serves as ready",
        );
        delay
    }
}

pub(crate) async fn sleep_or_cancel(delay: Duration, cancel: &mut watch::Receiver<bool>) -> bool {
    if *cancel.borrow() {
        return true;
    }
    tokio::select! {
        biased;
        _ = cancel.changed() => true,
        () = tokio::time::sleep(delay) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_source_stays_up_until_the_failure_threshold_then_lowers_the_aggregate() {
        let (health, mut rx) = InboundHealth::new();
        let mut tracker = HealthTracker::new("commands".to_string(), health, 3);
        assert!(*rx.borrow_and_update());
        tracker.failed(Duration::ZERO, DEFAULT_HEALTHY_UPTIME, "boom");
        assert!(*rx.borrow_and_update(), "one quick failure keeps the pod ready");
        tracker.failed(Duration::ZERO, DEFAULT_HEALTHY_UPTIME, "boom");
        tracker.failed(Duration::ZERO, DEFAULT_HEALTHY_UPTIME, "boom");
        assert!(
            !*rx.borrow_and_update(),
            "three quick failures cross the threshold and the pod goes DOWN"
        );
        tracker.promote();
        assert!(
            *rx.borrow_and_update(),
            "a serve that stayed up past the healthy window clears the aggregate"
        );
    }

    #[test]
    fn two_sources_both_clear_before_the_aggregate_recovers() {
        let (health, mut rx) = InboundHealth::new();
        let mut a = HealthTracker::new("a".to_string(), health.clone(), 1);
        let mut b = HealthTracker::new("b".to_string(), health, 1);
        a.failed(Duration::ZERO, DEFAULT_HEALTHY_UPTIME, "boom");
        b.failed(Duration::ZERO, DEFAULT_HEALTHY_UPTIME, "boom");
        assert!(!*rx.borrow_and_update());
        a.promote();
        assert!(!*rx.borrow_and_update(), "b is still down");
        b.promote();
        assert!(*rx.borrow_and_update(), "both up, so the aggregate recovers");
    }
}
