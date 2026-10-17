use serde::de::DeserializeOwned;
use serde_json::error::Category;

use crate::nats::NatsError;
use crate::nats::key::KvKey;

pub(crate) const UNDECODABLE_CODE: &str = "undecodable_value";

pub(crate) struct Undecodable(serde_json::Error);

impl Undecodable {
    fn new(error: serde_json::Error) -> Self {
        Self(error)
    }

    pub(crate) fn reason(&self) -> String {
        self.reason_for("the value")
    }

    pub(crate) fn reason_for(&self, subject: &str) -> String {
        let class = match self.0.classify() {
            Category::Io => "unreadable",
            Category::Syntax => "not valid json",
            Category::Data => "valid json of the wrong shape",
            Category::Eof => "truncated json",
        };
        format!(
            "{UNDECODABLE_CODE}: {subject} is {class} (line {}, column {})",
            self.0.line(),
            self.0.column()
        )
    }

    pub(crate) fn strict(self, key: &KvKey) -> NatsError {
        NatsError::Decode {
            key: key.as_str().to_string(),
            source: self.0,
        }
    }
}

pub(crate) enum Slot<V> {
    Value(V),
    Undecodable(Undecodable),
}

impl<V: DeserializeOwned> Slot<V> {
    pub(crate) fn from_json(bytes: &[u8]) -> Self {
        match serde_json::from_slice(bytes) {
            Ok(value) => Self::Value(value),
            Err(error) => Self::Undecodable(Undecodable::new(error)),
        }
    }
}

impl<V> Slot<V> {
    pub(crate) fn strict(self, key: &KvKey) -> Result<V, NatsError> {
        match self {
            Self::Value(value) => Ok(value),
            Self::Undecodable(fault) => Err(fault.strict(key)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct Shape {
        a: u32,
    }

    fn fault(bytes: &[u8]) -> Undecodable {
        Undecodable::new(serde_json::from_slice::<Shape>(bytes).unwrap_err())
    }

    #[test]
    fn the_reason_carries_a_stable_code_and_a_class_but_never_the_value() {
        let syntax = fault(b"{\"a\": secret-token-123}").reason();
        assert!(syntax.starts_with(UNDECODABLE_CODE), "{syntax}");
        assert!(syntax.contains("not valid json"), "{syntax}");
        assert!(!syntax.contains("secret-token-123"), "{syntax}");

        let shape = fault(b"\"secret-token-123\"").reason();
        assert!(shape.starts_with(UNDECODABLE_CODE), "{shape}");
        assert!(shape.contains("wrong shape"), "{shape}");
        assert!(!shape.contains("secret-token-123"), "{shape}");

        let truncated = fault(b"{\"a\": 1").reason();
        assert!(truncated.contains("truncated"), "{truncated}");
    }

    #[test]
    fn the_strict_conversion_keeps_the_public_decode_error() {
        let key = KvKey::new("catalog/item/1").unwrap();
        let error = Slot::<Shape>::Undecodable(fault(b"nope"))
            .strict(&key)
            .unwrap_err();
        assert!(matches!(error, NatsError::Decode { key, .. } if key == "catalog/item/1"));
        assert_eq!(Slot::Value(7_u32).strict(&key).unwrap(), 7);
    }
}
