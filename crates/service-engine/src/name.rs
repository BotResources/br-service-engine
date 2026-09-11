use std::borrow::Cow;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::EngineError;

const fn is_identifier(value: &str, max: usize) -> bool {
    let b = value.as_bytes();
    if b.is_empty() || b.len() > max {
        return false;
    }
    if !(b[0] >= b'a' && b[0] <= b'z') {
        return false;
    }
    let mut i = 1;
    while i < b.len() {
        let c = b[i];
        if !((c >= b'a' && c <= b'z') || (c >= b'0' && c <= b'9') || c == b'_') {
            return false;
        }
        i += 1;
    }
    true
}

const fn is_pod_id(value: &str, max: usize) -> bool {
    let b = value.as_bytes();
    if b.is_empty() || b.len() > max {
        return false;
    }
    if !((b[0] >= b'a' && b[0] <= b'z') || (b[0] >= b'0' && b[0] <= b'9')) {
        return false;
    }
    let mut i = 1;
    while i < b.len() {
        let c = b[i];
        if !((c >= b'a' && c <= b'z') || (c >= b'0' && c <= b'9') || c == b'-' || c == b'.') {
            return false;
        }
        i += 1;
    }
    true
}

const fn is_namespace(value: &str, max: usize) -> bool {
    let b = value.as_bytes();
    if b.is_empty() || b.len() > max {
        return false;
    }
    if b[0] == b'.' || b[b.len() - 1] == b'.' {
        return false;
    }
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if !((c >= b'a' && c <= b'z') || (c >= b'0' && c <= b'9') || c == b'.' || c == b'_') {
            return false;
        }
        if c == b'.' && i + 1 < b.len() && b[i + 1] == b'.' {
            return false;
        }
        i += 1;
    }
    true
}

const fn is_printable(value: &str, max: usize) -> bool {
    let b = value.as_bytes();
    if b.is_empty() || b.len() > max {
        return false;
    }
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c < 0x21 || c == 0x7f {
            return false;
        }
        i += 1;
    }
    true
}

macro_rules! validated_name {
    ($ty:ident, $kind:expr, $max:expr, $valid:path) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        pub struct $ty(Cow<'static, str>);

        impl $ty {
            pub const KIND: &'static str = $kind;
            pub const MAX_LEN: usize = $max;

            pub const fn from_static(value: &'static str) -> Self {
                assert!($valid(value, $max), concat!("invalid ", $kind));
                Self(Cow::Borrowed(value))
            }

            pub fn new(value: impl Into<String>) -> Result<Self, EngineError> {
                let value = value.into();
                if $valid(&value, $max) {
                    Ok(Self(Cow::Owned(value)))
                } else {
                    Err(EngineError::InvalidName { kind: $kind, value })
                }
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $ty {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let raw = String::deserialize(d)?;
                Self::new(raw).map_err(serde::de::Error::custom)
            }
        }
    };
}

validated_name!(NounName, "noun name", 64, is_identifier);
validated_name!(ProjectorName, "projector name", 64, is_identifier);
validated_name!(AccumulatorName, "accumulator name", 64, is_identifier);
validated_name!(RelayName, "relay name", 64, is_identifier);
validated_name!(JobName, "job name", 64, is_identifier);
validated_name!(MirrorName, "mirror name", 64, is_identifier);
validated_name!(ChannelName, "notify channel name", 63, is_identifier);
validated_name!(PodId, "pod id", 63, is_pod_id);
validated_name!(Namespace, "foreign namespace", 64, is_namespace);
validated_name!(ForeignId, "foreign key", 256, is_printable);

#[cfg(test)]
mod tests {
    use super::*;

    const STATIC_NOUN: NounName = NounName::from_static("assignment");

    #[test]
    fn a_static_name_is_validated_at_compile_time_and_equals_its_runtime_twin() {
        assert_eq!(STATIC_NOUN, NounName::new("assignment").unwrap());
        assert_eq!(STATIC_NOUN.as_str(), "assignment");
    }

    #[test]
    fn an_identifier_name_accepts_lowercase_snake_case_and_refuses_the_rest() {
        assert_eq!(
            NounName::new("assignment_v2").unwrap().as_str(),
            "assignment_v2"
        );
        assert!(NounName::new("").is_err());
        assert!(NounName::new("Assignment").is_err());
        assert!(NounName::new("2assignment").is_err());
        assert!(NounName::new("assignment-v2").is_err());
        assert!(NounName::new("a".repeat(65)).is_err());
    }

    #[test]
    fn a_channel_name_stays_within_the_postgres_identifier_budget() {
        assert!(ChannelName::new("a".repeat(63)).is_ok());
        assert!(ChannelName::new("a".repeat(64)).is_err());
    }

    #[test]
    fn a_pod_id_accepts_the_kubernetes_shape() {
        assert!(PodId::new("svc-chat-7d9f8c4b6-x2kqp").is_ok());
        assert!(PodId::new("0abc.def").is_ok());
        assert!(PodId::new("-leading-hyphen").is_err());
        assert!(PodId::new("Upper").is_err());
    }

    #[test]
    fn a_namespace_matches_the_directory_contract() {
        assert!(Namespace::new("identity.user").is_ok());
        assert!(Namespace::new(".identity").is_err());
        assert!(Namespace::new("identity.").is_err());
        assert!(Namespace::new("identity..user").is_err());
        assert!(Namespace::new("Identity").is_err());
    }

    #[test]
    fn a_foreign_id_refuses_control_characters_and_whitespace() {
        assert!(ForeignId::new("018f2b1a-0000-7000-8000-000000000000").is_ok());
        assert!(ForeignId::new("has space").is_err());
        assert!(ForeignId::new("has\nnewline").is_err());
        assert!(ForeignId::new("x".repeat(257)).is_err());
    }

    #[test]
    fn a_name_round_trips_through_serde_and_refuses_a_malformed_wire_value() {
        let name = ProjectorName::new("inbox").unwrap();
        let json = serde_json::to_string(&name).unwrap();
        assert_eq!(json, "\"inbox\"");
        assert_eq!(serde_json::from_str::<ProjectorName>(&json).unwrap(), name);
        assert!(serde_json::from_str::<ProjectorName>("\"Inbox\"").is_err());
    }
}
