use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

#[derive(Debug, Default)]
pub struct Stop {
    stopped: AtomicBool,
    notify: Notify,
}

impl Stop {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    pub async fn stopped(&self) {
        loop {
            if self.is_stopped() {
                return;
            }
            let waiting = self.notify.notified();
            tokio::pin!(waiting);
            waiting.as_mut().enable();
            if self.is_stopped() {
                return;
            }
            waiting.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_stop_raised_before_anyone_waits_is_still_seen_by_a_later_waiter() {
        let stop = Stop::new();
        stop.stop();
        tokio::time::timeout(std::time::Duration::from_secs(1), stop.stopped())
            .await
            .expect("a latched stop releases a waiter that arrives after it was raised");
    }

    #[tokio::test]
    async fn a_stop_releases_every_waiter_not_merely_one() {
        let stop = Stop::new();
        let waiters: Vec<_> = (0..4)
            .map(|_| {
                let stop = stop.clone();
                tokio::spawn(async move { stop.stopped().await })
            })
            .collect();
        tokio::task::yield_now().await;
        stop.stop();
        for waiter in waiters {
            tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
                .await
                .expect("every waiter is released by one stop")
                .expect("join");
        }
    }

    #[tokio::test]
    async fn a_task_that_is_never_polled_before_the_stop_still_ends() {
        let stop = Stop::new();
        let waiter = {
            let stop = stop.clone();
            tokio::spawn(async move { stop.stopped().await })
        };
        stop.stop();
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("a stop raised before the task's first poll is not lost")
            .expect("join");
    }

    #[tokio::test]
    async fn a_waiter_parks_while_no_stop_has_been_raised() {
        let stop = Stop::new();
        assert!(!stop.is_stopped());
        let parked =
            tokio::time::timeout(std::time::Duration::from_millis(50), stop.stopped()).await;
        assert!(
            parked.is_err(),
            "a waiter must park until the stop is raised"
        );
    }
}
