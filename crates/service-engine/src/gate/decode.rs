use std::collections::HashSet;
use std::fmt;
use std::sync::{Mutex, OnceLock};

use serde::de::{Deserialize, Deserializer, Error, MapAccess, Visitor};

use crate::gate::{ActionName, Affordances, Gate, Reason};

fn interner() -> &'static Mutex<HashSet<&'static str>> {
    static INTERNED: OnceLock<Mutex<HashSet<&'static str>>> = OnceLock::new();
    INTERNED.get_or_init(|| Mutex::new(HashSet::new()))
}

pub(crate) fn intern(value: &str) -> &'static str {
    let mut set = interner()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(existing) = set.get(value) {
        return existing;
    }
    let leaked: &'static str = Box::leak(value.to_owned().into_boxed_str());
    set.insert(leaked);
    leaked
}

impl<'de> Deserialize<'de> for Reason {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let code = String::deserialize(deserializer)?;
        if !crate::gate::is_reason_code(code.as_bytes()) {
            return Err(D::Error::custom(format!(
                "reason code {code:?} is not SCREAMING_SNAKE_CASE matching ^[A-Z][A-Z0-9_]+$"
            )));
        }
        Ok(Reason(intern(&code)))
    }
}

impl<'de> Deserialize<'de> for ActionName {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Ok(ActionName::from_static(intern(&name)))
    }
}

#[derive(serde::Deserialize)]
struct GateWire {
    allowed: bool,
    #[serde(default)]
    reason: Option<Reason>,
}

impl<'de> Deserialize<'de> for Gate {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = GateWire::deserialize(deserializer)?;
        if wire.allowed {
            Ok(Gate::allowed())
        } else {
            let reason = wire
                .reason
                .ok_or_else(|| D::Error::custom("a blocked gate carries a reason code"))?;
            Ok(Gate::blocked(reason))
        }
    }
}

struct AffordancesVisitor;

impl<'de> Visitor<'de> for AffordancesVisitor {
    type Value = Affordances;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a map of action name to gate")
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Affordances, M::Error> {
        let mut affordances = Affordances::new();
        while let Some((action, gate)) = map.next_entry::<ActionName, Gate>()? {
            affordances.insert(action, gate);
        }
        Ok(affordances)
    }
}

impl<'de> Deserialize<'de> for Affordances {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(AffordancesVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLOSE: ActionName = ActionName::from_static("close");
    const ALREADY_CLOSED: Reason = Reason::new("ALREADY_CLOSED");

    #[test]
    fn an_allowed_gate_round_trips_through_json() {
        let json = serde_json::to_string(&Gate::allowed()).unwrap();
        assert_eq!(
            serde_json::from_str::<Gate>(&json).unwrap(),
            Gate::allowed()
        );
    }

    #[test]
    fn a_blocked_gate_round_trips_its_reason_code() {
        let blocked = Gate::blocked(ALREADY_CLOSED);
        let json = serde_json::to_string(&blocked).unwrap();
        let back: Gate = serde_json::from_str(&json).unwrap();
        assert_eq!(back, blocked);
        assert_eq!(back.reason().map(|r| r.code()), Some("ALREADY_CLOSED"));
    }

    #[test]
    fn affordances_carrying_a_view_round_trip_through_the_rendered_store() {
        let affordances = Affordances::from_pairs([
            (CLOSE, Gate::blocked(ALREADY_CLOSED)),
            (ActionName::from_static("open"), Gate::allowed()),
        ]);
        let bytes = serde_json::to_vec(&affordances).unwrap();
        let back: Affordances = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back.get(CLOSE), Some(Gate::blocked(ALREADY_CLOSED)));
        assert_eq!(
            back.get(ActionName::from_static("open")),
            Some(Gate::allowed())
        );
    }

    #[test]
    fn interning_a_repeated_code_yields_one_stable_static_pointer() {
        let first = intern("recomputed_at_runtime");
        let second = intern(&String::from("recomputed_at_runtime"));
        assert!(std::ptr::eq(first, second));
    }
}
