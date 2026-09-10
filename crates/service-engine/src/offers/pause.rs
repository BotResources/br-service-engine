use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

struct Gate {
    armed: AtomicBool,
    reached: Notify,
    release: Notify,
}

static GATES: Mutex<Vec<Arc<Gate>>> = Mutex::new(Vec::new());

pub struct OfferDrainGate(Arc<Gate>);

impl OfferDrainGate {
    pub async fn reached(&self) {
        self.0.reached.notified().await;
    }

    pub fn release(self) {
        self.0.release.notify_one();
    }
}

pub fn arm_offer_drain() -> OfferDrainGate {
    let gate = Arc::new(Gate {
        armed: AtomicBool::new(true),
        reached: Notify::new(),
        release: Notify::new(),
    });
    GATES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(gate.clone());
    OfferDrainGate(gate)
}

pub(crate) async fn wait() {
    let hit = {
        let guard = GATES
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard
            .iter()
            .find(|gate| gate.armed.swap(false, Ordering::SeqCst))
            .cloned()
    };
    if let Some(gate) = hit {
        gate.reached.notify_one();
        gate.release.notified().await;
    }
}
