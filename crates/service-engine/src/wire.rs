use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::EngineError;
use crate::name::NounName;

macro_rules! opaque_json {
    ($ty:ident, $what:expr) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $ty(Bytes);

        impl $ty {
            pub const WHAT: &'static str = $what;

            pub fn encode<T: Serialize>(value: &T) -> Result<Self, EngineError> {
                Ok(Self(canonical_json(value, $what)?))
            }

            pub fn from_json_bytes(bytes: Bytes) -> Result<Self, EngineError> {
                let value: serde_json::Value =
                    serde_json::from_slice(&bytes).map_err(|source| EngineError::Decode {
                        what: $what,
                        source,
                    })?;
                Self::encode(&value)
            }

            pub fn decode<T: DeserializeOwned>(&self) -> Result<T, EngineError> {
                serde_json::from_slice(&self.0).map_err(|source| EngineError::Decode {
                    what: $what,
                    source,
                })
            }

            pub fn as_bytes(&self) -> &Bytes {
                &self.0
            }

            pub fn as_slice(&self) -> &[u8] {
                &self.0
            }

            pub fn len(&self) -> usize {
                self.0.len()
            }

            pub fn is_empty(&self) -> bool {
                self.0.is_empty()
            }
        }

        impl Serialize for $ty {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                let value: serde_json::Value =
                    serde_json::from_slice(&self.0).map_err(serde::ser::Error::custom)?;
                value.serialize(s)
            }
        }

        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let value = serde_json::Value::deserialize(d)?;
                Self::encode(&value).map_err(serde::de::Error::custom)
            }
        }
    };
}

opaque_json!(KeyBytes, "key");
opaque_json!(ViewBytes, "view");
opaque_json!(Cause, "cause");

fn canonical_json<T: Serialize>(value: &T, what: &'static str) -> Result<Bytes, EngineError> {
    let value =
        serde_json::to_value(value).map_err(|source| EngineError::Encode { what, source })?;
    let mut out = Vec::new();
    write_canonical(&value, what, &mut out)?;
    Ok(Bytes::from(out))
}

fn write_canonical(
    value: &serde_json::Value,
    what: &'static str,
    out: &mut Vec<u8>,
) -> Result<(), EngineError> {
    let scalar = |value: &serde_json::Value, out: &mut Vec<u8>| {
        serde_json::to_writer(out, value).map_err(|source| EngineError::Encode { what, source })
    };
    match value {
        serde_json::Value::Object(map) => {
            let mut fields: Vec<(&String, &serde_json::Value)> = map.iter().collect();
            fields.sort_by(|left, right| left.0.cmp(right.0));
            out.push(b'{');
            for (index, (key, field)) in fields.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                scalar(&serde_json::Value::String(key.clone()), out)?;
                out.push(b':');
                write_canonical(field, what, out)?;
            }
            out.push(b'}');
        }
        serde_json::Value::Array(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_canonical(item, what, out)?;
            }
            out.push(b']');
        }
        other => scalar(other, out)?,
    }
    Ok(())
}

pub trait Noun: 'static {
    type Key: Clone + Ord + std::hash::Hash + Send + Sync + Serialize + DeserializeOwned + 'static;
    const NAME: NounName;
}

pub fn encode_key<N: Noun>(key: &N::Key) -> Result<KeyBytes, EngineError> {
    KeyBytes::encode(key)
}

pub fn decode_key<N: Noun>(key: &KeyBytes) -> Result<N::Key, EngineError> {
    key.decode()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    struct AssignmentKey {
        zebra: u32,
        alpha: String,
    }

    struct Assignment;
    impl Noun for Assignment {
        type Key = AssignmentKey;
        const NAME: NounName = NounName::from_static("assignment");
    }

    #[test]
    fn a_key_encodes_canonically_with_sorted_object_fields() {
        let key = AssignmentKey {
            zebra: 7,
            alpha: "a".into(),
        };
        let encoded = encode_key::<Assignment>(&key).unwrap();
        assert_eq!(
            std::str::from_utf8(encoded.as_slice()).unwrap(),
            r#"{"alpha":"a","zebra":7}"#
        );
        assert_eq!(decode_key::<Assignment>(&encoded).unwrap(), key);
    }

    #[test]
    fn two_maps_that_differ_only_in_insertion_order_encode_to_the_same_key() {
        let mut one = BTreeMap::new();
        one.insert("b", 2);
        one.insert("a", 1);
        let mut two = BTreeMap::new();
        two.insert("a", 1);
        two.insert("b", 2);
        assert_eq!(
            KeyBytes::encode(&one).unwrap(),
            KeyBytes::encode(&two).unwrap()
        );
    }

    #[test]
    fn nested_object_fields_are_sorted_at_every_depth_not_only_the_top_level() {
        let value = serde_json::json!({
            "outer_z": {"inner_z": 1, "inner_a": 2},
            "outer_a": [{"b": 1, "a": 2}],
        });
        let encoded = KeyBytes::encode(&value).unwrap();
        assert_eq!(
            std::str::from_utf8(encoded.as_slice()).unwrap(),
            r#"{"outer_a":[{"a":2,"b":1}],"outer_z":{"inner_a":2,"inner_z":1}}"#,
            "the canonical form sorts object keys recursively, independent of serde_json map order"
        );
    }

    #[test]
    fn a_key_survives_the_notify_payload_round_trip_as_json_not_as_a_byte_array() {
        let key = encode_key::<Assignment>(&AssignmentKey {
            zebra: 1,
            alpha: "x".into(),
        })
        .unwrap();
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, r#"{"alpha":"x","zebra":1}"#);
        assert_eq!(serde_json::from_str::<KeyBytes>(&json).unwrap(), key);
    }

    #[test]
    fn decoding_a_key_into_the_wrong_type_fails_typed() {
        let encoded = KeyBytes::encode(&AssignmentKey {
            zebra: 1,
            alpha: "x".into(),
        })
        .unwrap();
        let wrong = encoded.decode::<u64>();
        assert!(matches!(
            wrong,
            Err(EngineError::Decode { what: "key", .. })
        ));
    }

    #[test]
    fn raw_bytes_that_are_not_json_are_refused_at_construction() {
        assert!(KeyBytes::from_json_bytes(Bytes::from_static(b"not json")).is_err());
        assert!(KeyBytes::from_json_bytes(Bytes::from_static(b"{\"a\":1}")).is_ok());
    }

    #[test]
    fn a_cause_carries_an_opaque_domain_event_through_the_engine() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Assigned {
            to: String,
        }
        let cause = Cause::encode(&Assigned { to: "jx".into() }).unwrap();
        assert_eq!(
            cause.decode::<Assigned>().unwrap(),
            Assigned { to: "jx".into() }
        );
    }
}
