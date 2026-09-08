use serde::{Deserialize, Serialize};

use crate::sample::counter::domain::CounterError;

pub const EVENT_VERSION: i32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum CounterEvent {
    Bumped { amount: i64, author: String },
    Closed,
}

pub fn upcast(version: i32, payload: &serde_json::Value) -> Result<CounterEvent, CounterError> {
    if version >= EVENT_VERSION {
        return serde_json::from_value(payload.clone())
            .map_err(|error| CounterError::Hydration(format!("event v{version} decode: {error}")));
    }
    if version == 1 {
        return upcast_v1(payload);
    }
    Err(CounterError::Hydration(format!(
        "unknown event version {version}"
    )))
}

fn upcast_v1(payload: &serde_json::Value) -> Result<CounterEvent, CounterError> {
    match payload.get("kind").and_then(serde_json::Value::as_str) {
        Some("Bumped") => {
            let amount = payload
                .get("by")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| CounterError::Hydration("v1 Bumped has no integer 'by'".into()))?;
            let author = payload
                .get("who")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            Ok(CounterEvent::Bumped { amount, author })
        }
        Some("Closed") => Ok(CounterEvent::Closed),
        other => Err(CounterError::Hydration(format!(
            "unknown v1 event kind {other:?}"
        ))),
    }
}
