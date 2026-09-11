use std::time::{Duration, Instant};

use br_core_integration::retry_backoff;

#[derive(Debug, Clone, Default)]
pub struct Backoff {
    attempts: u32,
    ready_at: Option<Instant>,
}

impl Backoff {
    pub fn is_ready(&self, now: Instant) -> bool {
        self.ready_at.is_none_or(|at| now >= at)
    }

    pub fn fail(&mut self, now: Instant) -> Duration {
        self.attempts = self.attempts.saturating_add(1);
        let wait = retry_backoff(self.attempts);
        self.ready_at = Some(now + wait);
        wait
    }

    pub fn succeed(&mut self) {
        self.attempts = 0;
        self.ready_at = None;
    }

    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    pub fn retry_in(&self, now: Instant) -> Duration {
        match self.ready_at {
            Some(at) => at.saturating_duration_since(now),
            None => Duration::ZERO,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_backoff_lets_its_relay_run_immediately() {
        let now = Instant::now();
        let backoff = Backoff::default();
        assert!(backoff.is_ready(now));
        assert_eq!(backoff.attempts(), 0);
        assert_eq!(backoff.retry_in(now), Duration::ZERO);
    }

    #[test]
    fn a_failure_holds_the_relay_back_for_the_shared_platform_backoff() {
        let now = Instant::now();
        let mut backoff = Backoff::default();
        assert_eq!(backoff.fail(now), retry_backoff(1));
        assert!(!backoff.is_ready(now));
        assert!(backoff.is_ready(now + retry_backoff(1)));
        assert_eq!(backoff.fail(now), retry_backoff(2));
        assert_eq!(backoff.attempts(), 2);
    }

    #[test]
    fn a_success_clears_the_hold_and_the_attempt_count() {
        let now = Instant::now();
        let mut backoff = Backoff::default();
        backoff.fail(now);
        backoff.fail(now);
        backoff.succeed();
        assert!(backoff.is_ready(now));
        assert_eq!(backoff.attempts(), 0);
    }

    #[test]
    fn the_hold_never_overflows_however_long_a_relay_keeps_failing() {
        let now = Instant::now();
        let mut backoff = Backoff::default();
        for _ in 0..64 {
            backoff.fail(now);
        }
        assert_eq!(backoff.retry_in(now), retry_backoff(u32::MAX));
    }
}
