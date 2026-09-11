use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use uuid::Uuid;

#[derive(Debug, Default)]
pub struct Spy {
    pub(super) populates: AtomicUsize,
    pub(super) loads: AtomicUsize,
    pub(super) projects: AtomicUsize,
    pub(super) loaded: std::sync::Mutex<Vec<BTreeSet<Uuid>>>,
}

impl Spy {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn populates(&self) -> usize {
        self.populates.load(Ordering::Relaxed)
    }

    pub fn loads(&self) -> usize {
        self.loads.load(Ordering::Relaxed)
    }

    pub fn projects(&self) -> usize {
        self.projects.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.populates.store(0, Ordering::Relaxed);
        self.loads.store(0, Ordering::Relaxed);
        self.projects.store(0, Ordering::Relaxed);
        self.loaded.lock().unwrap().clear();
    }

    pub fn loaded_rows(&self) -> Vec<BTreeSet<Uuid>> {
        self.loaded.lock().unwrap().clone()
    }

    pub fn ever_loaded(&self, id: Uuid) -> bool {
        self.loaded_rows().iter().any(|set| set.contains(&id))
    }
}
