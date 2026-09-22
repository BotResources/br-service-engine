use std::any::TypeId;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::EngineError;
use crate::nats::{KV_PUBLISHED_LANGUAGE, KvKey};

pub trait Consumed: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
    const PREFIX: &'static str;

    const VERSION: u16 = 1;

    const RAW_JSON_ESCAPE_HATCH: bool = false;

    fn bucket() -> &'static str {
        KV_PUBLISHED_LANGUAGE
    }

    fn wire_version(_value: &Self) -> Option<u16> {
        None
    }

    fn manifest() -> ConsumedManifest {
        ConsumedManifest::new(Self::PREFIX, Self::VERSION)
    }
}

pub fn is_raw_json<C: Consumed>() -> bool {
    TypeId::of::<C>() == TypeId::of::<serde_json::Value>()
}

pub fn manifest_key(prefix: &str) -> Result<KvKey, EngineError> {
    let base = prefix.strip_suffix('/').ok_or_else(|| {
        EngineError::Config(format!(
            "prefix {prefix:?} must end with '/' so its manifest key sits outside the data prefix"
        ))
    })?;
    KvKey::new(format!("{base}_manifest"))
        .map_err(|error| EngineError::Config(format!("manifest key for prefix {prefix}: {error}")))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OfferManifest {
    pub prefix: String,
    pub version: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumedManifest {
    pub offer: &'static str,
    pub version: u16,
}

impl ConsumedManifest {
    pub const fn new(offer: &'static str, version: u16) -> Self {
        Self { offer, version }
    }

    pub fn accepts(&self, found_offer: &str, found_version: u16) -> Result<(), ManifestMismatch> {
        if self.offer != found_offer {
            return Err(ManifestMismatch::Offer {
                expected: self.offer,
                found: found_offer.to_string(),
            });
        }
        if self.version != found_version {
            return Err(ManifestMismatch::Version {
                offer: self.offer,
                expected: self.version,
                found: found_version,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManifestMismatch {
    #[error(
        "offer {found} was published under the prefix this consumer reads for offer {expected}"
    )]
    Offer {
        expected: &'static str,
        found: String,
    },

    #[error(
        "offer {offer} was published at wire version {found}, but this consumer is coded against \
         version {expected}; a breaking change moves the version into a new prefix"
    )]
    Version {
        offer: &'static str,
        expected: u16,
        found: u16,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Typed {
        #[allow(dead_code)]
        value: u32,
    }
    impl Consumed for Typed {
        const PREFIX: &'static str = "typed/v1/";
        const VERSION: u16 = 1;
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct RawEscaped(#[allow(dead_code)] serde_json::Value);
    impl Consumed for RawEscaped {
        const PREFIX: &'static str = "raw/v1/";
    }

    impl Consumed for serde_json::Value {
        const PREFIX: &'static str = "loose/v1/";
    }

    #[test]
    fn a_typed_value_is_never_raw_json() {
        assert!(!is_raw_json::<Typed>());
        assert!(!is_raw_json::<RawEscaped>());
    }

    #[test]
    fn the_bare_json_value_is_the_only_raw_json_consumption() {
        assert!(is_raw_json::<serde_json::Value>());
    }

    #[test]
    fn a_manifest_key_sits_outside_the_data_prefix() {
        let key = manifest_key("typed/v1/").unwrap();
        assert_eq!(key.as_str(), "typed/v1_manifest");
        assert!(!key.as_str().starts_with("typed/v1/"));
    }

    #[test]
    fn a_manifest_key_for_a_prefix_without_a_trailing_separator_is_refused() {
        let refusal = manifest_key("identity/users").unwrap_err();
        assert!(matches!(refusal, EngineError::Config(_)));
    }

    #[test]
    fn a_manifest_accepts_the_same_offer_at_the_same_version() {
        assert!(Typed::manifest().accepts("typed/v1/", 1).is_ok());
    }

    #[test]
    fn a_manifest_rejects_a_moved_version_on_the_same_prefix() {
        let verdict = Typed::manifest().accepts("typed/v1/", 2);
        assert_eq!(
            verdict,
            Err(ManifestMismatch::Version {
                offer: "typed/v1/",
                expected: 1,
                found: 2,
            })
        );
    }

    #[test]
    fn a_manifest_rejects_a_different_offer_under_the_prefix() {
        let verdict = Typed::manifest().accepts("other/v1/", 1);
        assert_eq!(
            verdict,
            Err(ManifestMismatch::Offer {
                expected: "typed/v1/",
                found: "other/v1/".to_string(),
            })
        );
    }

    #[test]
    fn wire_version_defaults_to_none() {
        assert_eq!(Typed::wire_version(&Typed { value: 1 }), None);
    }
}
