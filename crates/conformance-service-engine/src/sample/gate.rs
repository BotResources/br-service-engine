use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Semaphore;

#[derive(Debug)]
pub struct Gate {
    remaining: AtomicUsize,
    entered: Semaphore,
    released: Semaphore,
}

impl Default for Gate {
    fn default() -> Self {
        Self {
            remaining: AtomicUsize::new(1),
            entered: Semaphore::new(0),
            released: Semaphore::new(0),
        }
    }
}

impl Gate {
    pub fn new() -> Arc<Self> {
        Self::times(1)
    }

    pub fn times(count: usize) -> Arc<Self> {
        Arc::new(Self {
            remaining: AtomicUsize::new(count),
            ..Self::default()
        })
    }

    pub async fn pass(&self) {
        if !take_one(&self.remaining) {
            return;
        }
        self.entered.add_permits(1);
        self.released
            .acquire()
            .await
            .expect("the gate outlives the call it holds")
            .forget();
    }

    pub async fn wait_until_inside(&self) {
        self.entered
            .acquire()
            .await
            .expect("the gate outlives the call it holds")
            .forget();
    }

    pub fn release(&self) {
        self.released.add_permits(1);
    }
}

fn take_one(counter: &AtomicUsize) -> bool {
    let mut left = counter.load(Ordering::SeqCst);
    while left > 0 {
        match counter.compare_exchange(left, left - 1, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return true,
            Err(actual) => left = actual,
        }
    }
    false
}
