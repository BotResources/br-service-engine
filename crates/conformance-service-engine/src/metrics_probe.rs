use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use metrics::{
    Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};

type Totals = Arc<Mutex<HashMap<String, u64>>>;

#[derive(Clone, Default)]
pub struct MetricProbe {
    totals: Totals,
}

struct ProbeCounter {
    slot: String,
    totals: Totals,
}

impl CounterFn for ProbeCounter {
    fn increment(&self, value: u64) {
        let mut totals = self.totals.lock().unwrap_or_else(|p| p.into_inner());
        *totals.entry(self.slot.clone()).or_default() += value;
    }

    fn absolute(&self, value: u64) {
        let mut totals = self.totals.lock().unwrap_or_else(|p| p.into_inner());
        let entry = totals.entry(self.slot.clone()).or_default();
        *entry = (*entry).max(value);
    }
}

fn slot(key: &Key) -> String {
    let mut labels: Vec<String> = key
        .labels()
        .map(|label| format!("{}={}", label.key(), label.value()))
        .collect();
    labels.sort();
    if labels.is_empty() {
        key.name().to_string()
    } else {
        format!("{}{{{}}}", key.name(), labels.join(","))
    }
}

impl MetricProbe {
    pub fn total(&self, name: &str) -> u64 {
        *self
            .totals
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(name)
            .unwrap_or(&0)
    }

    pub fn labelled_total(&self, name: &str, label_key: &str, label_value: &str) -> u64 {
        self.total(&format!("{name}{{{label_key}={label_value}}}"))
    }
}

impl Recorder for MetricProbe {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(ProbeCounter {
            slot: slot(key),
            totals: self.totals.clone(),
        }))
    }

    fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

static PROBE: OnceLock<MetricProbe> = OnceLock::new();

pub fn install() -> MetricProbe {
    let probe = PROBE.get_or_init(MetricProbe::default).clone();
    let _ = metrics::set_global_recorder(probe.clone());
    probe
}
