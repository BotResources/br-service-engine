use std::any::TypeId;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::nats::KV_PUBLISHED_LANGUAGE;

/// A typed value a mirror consumes from a producer's KV offer. The type pins the
/// wire: only a payload that deserializes into `Self` enters a shadow, so the
/// join reads typed rows and never a `serde_json::Value` (see [`is_raw_json`],
/// the registration-time refusal that keeps it so).
pub trait Consumed: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
    /// The key prefix this consumer reads under. The offer's version travels in
    /// its key for a breaking change, so two versions of one offer are two
    /// prefixes; [`Consumed::VERSION`] then names the wire this code is written
    /// against, and the manifest check will make a producer that reused a prefix
    /// for an incompatible version a nameable dead letter instead of silent
    /// mis-decoding.
    const PREFIX: &'static str;

    /// The wire version of the offer this consumer is coded against. The engine
    /// will compare it to the producer-owned manifest at scan and at watch (the
    /// verdict is [`ConsumedManifest::accepts`]; the scan/watch enforcement that
    /// applies it is not yet wired — it lands in the mirror runtime); a mismatch
    /// dead-letters that key and never touches readiness, because content is
    /// never a readiness input and an empty prefix stays converged.
    const VERSION: u16 = 1;

    /// Escape hatch for a value that is deliberately raw JSON because the
    /// producer's column is itself JSON. Leave it `false`: the engine refuses a
    /// `serde_json::Value` consumption at registration otherwise, so a slice
    /// cannot silently opt out of typing the whole value. A typed struct that
    /// merely holds a `serde_json::Value` *field* needs no escape hatch — only a
    /// consumption whose entire value is untyped does.
    ///
    /// The refusal is [`is_raw_json`], which matches the **bare**
    /// `serde_json::Value` by type id. A `#[serde(transparent)]` newtype wrapping
    /// a `Value` (`struct Raw(serde_json::Value)`) is a distinct type, so it is
    /// not caught and is an unpoliced whole-value untyped consumption — treat it
    /// as taking this escape without setting the flag, and prefer a real typed
    /// struct or this hatch on the bare value instead.
    const RAW_JSON_ESCAPE_HATCH: bool = false;

    fn bucket() -> &'static str {
        KV_PUBLISHED_LANGUAGE
    }

    /// The manifest this consumer expects the producer's offer to carry.
    fn manifest() -> ConsumedManifest {
        ConsumedManifest::new(Self::PREFIX, Self::VERSION)
    }
}

/// True when the consumed value is the **bare** `serde_json::Value` — the
/// untyped escape a mirror must not take unless it says so explicitly. A
/// `Consumed` bound is `'static`, so the type id is always available. This
/// matches the type id exactly: a `#[serde(transparent)]` newtype over `Value`
/// is a distinct type and is not caught (see [`Consumed::RAW_JSON_ESCAPE_HATCH`]).
pub fn is_raw_json<C: Consumed>() -> bool {
    TypeId::of::<C>() == TypeId::of::<serde_json::Value>()
}

/// The offer identity a manifest names: the offer's prefix and the wire version
/// under it. A consumer builds the expected manifest from its [`Consumed`]
/// constants; the producer publishes the found one; the engine compares them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumedManifest {
    pub offer: &'static str,
    pub version: u16,
}

impl ConsumedManifest {
    pub const fn new(offer: &'static str, version: u16) -> Self {
        Self { offer, version }
    }

    /// The pure verdict the engine reaches at scan and watch. A found manifest
    /// is accepted only when it names the same offer at the same version; any
    /// other outcome is the reason a dead letter carries for that key. The found
    /// side owns its strings because it comes off the wire.
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

/// Why a producer-owned manifest did not match the consumer's expectation. It is
/// a dead-letter reason, never a readiness input.
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

    // The bare value carries no escape hatch, so a mirror that consumes it is
    // refused at registration (see the builder's guard test).
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
}
