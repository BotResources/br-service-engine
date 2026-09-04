use serde::{Deserialize, Serialize};

use crate::error::EngineError;
use crate::name::{ForeignId, Namespace, NounName};
use crate::principal::PrincipalId;
use crate::wire::{Cause, KeyBytes, Noun, encode_key};

macro_rules! bit_set {
    ($ty:ident) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $ty(u32);

        impl $ty {
            pub const EMPTY: Self = Self(0);
            pub const ALL: Self = Self(u32::MAX);
            pub const WIDTH: u8 = 32;

            pub fn bit(index: u8) -> Result<Self, EngineError> {
                if index >= Self::WIDTH {
                    return Err(EngineError::BitOutOfRange(index));
                }
                Ok(Self(1u32 << index))
            }

            pub const fn from_bits(bits: u32) -> Self {
                Self(bits)
            }

            pub const fn bits(self) -> u32 {
                self.0
            }

            pub const fn union(self, other: Self) -> Self {
                Self(self.0 | other.0)
            }

            pub const fn intersects(self, other: Self) -> bool {
                self.0 & other.0 != 0
            }

            pub const fn contains(self, other: Self) -> bool {
                self.0 & other.0 == other.0
            }

            pub const fn is_empty(self) -> bool {
                self.0 == 0
            }
        }

        impl Default for $ty {
            fn default() -> Self {
                Self::EMPTY
            }
        }
    };
}

bit_set!(Dims);
bit_set!(Deps);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ForeignKey {
    namespace: Namespace,
    key: ForeignId,
}

impl ForeignKey {
    pub fn new(namespace: &str, key: &str) -> Result<Self, EngineError> {
        Ok(Self {
            namespace: Namespace::new(namespace)?,
            key: ForeignId::new(key)?,
        })
    }

    pub fn from_parts(namespace: Namespace, key: ForeignId) -> Self {
        Self { namespace, key }
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    pub fn key(&self) -> &ForeignId {
        &self.key
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum Impact {
    ResourceChanged {
        noun: NounName,
        key: KeyBytes,
        dims: Dims,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cause: Option<Cause>,
    },
    PrincipalFactsChanged {
        principal: PrincipalId,
        deps: Deps,
    },
    ForeignChanged {
        foreign: ForeignKey,
    },
}

impl Impact {
    pub fn resource<N: Noun>(key: &N::Key, dims: Dims) -> Result<Self, EngineError> {
        Ok(Self::ResourceChanged {
            noun: N::NAME,
            key: encode_key::<N>(key)?,
            dims,
            cause: None,
        })
    }

    pub fn resource_caused<N: Noun>(
        key: &N::Key,
        dims: Dims,
        cause: Cause,
    ) -> Result<Self, EngineError> {
        Ok(Self::ResourceChanged {
            noun: N::NAME,
            key: encode_key::<N>(key)?,
            dims,
            cause: Some(cause),
        })
    }

    pub fn principal_facts(principal: PrincipalId, deps: Deps) -> Self {
        Self::PrincipalFactsChanged { principal, deps }
    }

    pub fn foreign(foreign: ForeignKey) -> Self {
        Self::ForeignChanged { foreign }
    }

    pub fn cause(&self) -> Option<&Cause> {
        match self {
            Self::ResourceChanged { cause, .. } => cause.as_ref(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransportEvent {
    Impacts(Vec<Impact>),
    Reconnected,
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Assignment;
    impl Noun for Assignment {
        type Key = uuid::Uuid;
        const NAME: NounName = NounName::from_static("assignment");
    }

    #[test]
    fn a_bit_set_folds_and_intersects() {
        let a = Dims::bit(0).unwrap();
        let b = Dims::bit(3).unwrap();
        let both = a.union(b);
        assert!(both.contains(a));
        assert!(both.intersects(b));
        assert!(!a.intersects(b));
        assert!(Dims::EMPTY.is_empty());
        assert!(!Dims::EMPTY.intersects(Dims::ALL));
    }

    #[test]
    fn a_bit_beyond_the_set_width_is_refused() {
        assert!(matches!(Dims::bit(32), Err(EngineError::BitOutOfRange(32))));
        assert!(Deps::bit(31).is_ok());
    }

    #[test]
    fn an_impact_addresses_a_noun_and_carries_no_revision() {
        let key = uuid::Uuid::now_v7();
        let impact = Impact::resource::<Assignment>(&key, Dims::bit(1).unwrap()).unwrap();
        let Impact::ResourceChanged { noun, cause, .. } = &impact else {
            panic!("expected a ResourceChanged");
        };
        assert_eq!(noun, &NounName::from_static("assignment"));
        assert!(cause.is_none());
    }

    #[test]
    fn an_impact_round_trips_through_the_notify_payload_encoding() {
        let key = uuid::Uuid::now_v7();
        let caused = Impact::resource_caused::<Assignment>(
            &key,
            Dims::ALL,
            Cause::encode(&"assigned").unwrap(),
        )
        .unwrap();
        for impact in [
            Impact::resource::<Assignment>(&key, Dims::EMPTY).unwrap(),
            caused,
            Impact::principal_facts(uuid::Uuid::now_v7().into(), Deps::bit(2).unwrap()),
            Impact::foreign(ForeignKey::new("identity.user", &key.to_string()).unwrap()),
        ] {
            let json = serde_json::to_string(&impact).unwrap();
            assert_eq!(serde_json::from_str::<Impact>(&json).unwrap(), impact);
        }
    }

    #[test]
    fn a_foreign_key_validates_both_segments_at_construction() {
        assert!(ForeignKey::new("identity.user", "abc").is_ok());
        assert!(ForeignKey::new("Identity", "abc").is_err());
        assert!(ForeignKey::new("identity.user", "a b").is_err());
    }
}
