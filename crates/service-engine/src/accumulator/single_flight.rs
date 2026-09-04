use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use tokio::sync::Mutex as AsyncMutex;

pub(crate) struct KeyedGate<K> {
    gates: Mutex<HashMap<K, Arc<AsyncMutex<()>>>>,
}

struct GateEviction<'a, K: Eq + Hash> {
    gates: &'a Mutex<HashMap<K, Arc<AsyncMutex<()>>>>,
    key: K,
    gate: Arc<AsyncMutex<()>>,
}

impl<K: Eq + Hash> Drop for GateEviction<'_, K> {
    fn drop(&mut self) {
        let mut gates = self.gates.lock().unwrap_or_else(|p| p.into_inner());
        if Arc::strong_count(&self.gate) == 2 {
            gates.remove(&self.key);
        }
    }
}

impl<K: Clone + Eq + Hash> KeyedGate<K> {
    pub(crate) fn new() -> Self {
        Self {
            gates: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn run<F, T>(&self, key: K, work: F) -> T
    where
        F: Future<Output = T>,
    {
        let gate = self
            .gates
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(key.clone())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone();
        let eviction = GateEviction {
            gates: &self.gates,
            key,
            gate,
        };
        let _held = eviction.gate.lock().await;
        work.await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    async fn observe_peak(gate: Arc<KeyedGate<u8>>, keys: [u8; 3]) -> usize {
        let inflight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for key in keys {
            let gate = gate.clone();
            let inflight = inflight.clone();
            let peak = peak.clone();
            tasks.push(tokio::spawn(async move {
                gate.run(key, async {
                    let now = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    inflight.fetch_sub(1, Ordering::SeqCst);
                })
                .await;
            }));
        }
        for task in tasks {
            task.await.expect("the gated task joins");
        }
        peak.load(Ordering::SeqCst)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_callers_on_one_key_never_run_the_work_at_the_same_time() {
        let gate = Arc::new(KeyedGate::new());
        assert_eq!(
            observe_peak(gate, [7, 7, 7]).await,
            1,
            "a second read of the same key waits for the first fold instead of refolding beside it"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn callers_on_distinct_keys_run_side_by_side() {
        let gate = Arc::new(KeyedGate::new());
        assert!(
            observe_peak(gate, [1, 2, 3]).await > 1,
            "distinct keys are independent, so one key's fold never blocks another's"
        );
    }

    #[tokio::test]
    async fn a_settled_key_leaves_no_gate_behind() {
        let gate: KeyedGate<u8> = KeyedGate::new();
        gate.run(9, async {}).await;
        assert!(
            gate.gates
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty(),
            "the last caller through a key drops its gate so the map cannot grow without bound"
        );
    }

    #[tokio::test]
    async fn a_fold_cancelled_mid_flight_leaves_no_gate_behind() {
        let gate: KeyedGate<u8> = KeyedGate::new();
        let cut_off = tokio::time::timeout(
            Duration::from_millis(20),
            gate.run(4, async {
                tokio::time::sleep(Duration::from_millis(400)).await;
            }),
        )
        .await;
        assert!(cut_off.is_err(), "the fold is dropped before it settles");
        assert!(
            gate.gates
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty(),
            "a fold dropped mid-flight still evicts its gate so the cancel path cannot leak"
        );
    }
}
