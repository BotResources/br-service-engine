use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::error::EngineError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealHash([u8; 32]);

impl SealHash {
    pub fn of_chunks<C: Serialize>(
        chunks: impl IntoIterator<Item = C>,
    ) -> Result<Self, EngineError> {
        let mut hasher = Sha256::new();
        for chunk in chunks {
            let bytes = serde_json::to_vec(&chunk).map_err(|source| EngineError::Encode {
                what: "seal chunk",
                source,
            })?;
            hasher.update(&bytes);
        }
        Ok(Self(hasher.finalize().into()))
    }

    pub(crate) fn of_values<'a>(values: impl IntoIterator<Item = &'a serde_json::Value>) -> Self {
        let mut hasher = Sha256::new();
        for value in values {
            let bytes = serde_json::to_vec(value).unwrap_or_default();
            hasher.update(&bytes);
        }
        Self(hasher.finalize().into())
    }

    pub fn to_hex(self) -> String {
        let mut out = String::with_capacity(64);
        for byte in self.0 {
            out.push_str(&format!("{byte:02x}"));
        }
        out
    }

    pub fn from_hex(raw: &str) -> Result<Self, EngineError> {
        if raw.len() != 64 {
            return Err(EngineError::SealHashFormat(format!(
                "expected 64 hex characters, got {}",
                raw.len()
            )));
        }
        let mut bytes = [0u8; 32];
        for (index, pair) in raw.as_bytes().chunks_exact(2).enumerate() {
            let hex = std::str::from_utf8(pair)
                .map_err(|_| EngineError::SealHashFormat("non-utf8 hex".into()))?;
            bytes[index] = u8::from_str_radix(hex, 16)
                .map_err(|error| EngineError::SealHashFormat(error.to_string()))?;
        }
        Ok(Self(bytes))
    }
}

impl Serialize for SealHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for SealHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::from_hex(&raw).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_chunks_hash_the_same_whether_typed_or_as_json_values() {
        let typed =
            SealHash::of_chunks(["Hel".to_string(), "lo ".to_string(), "world".to_string()])
                .expect("typed chunks hash");
        let values = [
            serde_json::json!("Hel"),
            serde_json::json!("lo "),
            serde_json::json!("world"),
        ];
        let from_values = SealHash::of_values(values.iter());
        assert_eq!(typed, from_values);
    }

    #[test]
    fn a_hash_round_trips_through_its_hex_form() {
        let hash = SealHash::of_chunks(["one", "two"]).expect("hash");
        let hex = hash.to_hex();
        assert_eq!(hex.len(), 64);
        assert_eq!(SealHash::from_hex(&hex).expect("parse"), hash);
    }

    #[test]
    fn a_malformed_hex_seal_hash_is_refused() {
        assert!(SealHash::from_hex("nothex").is_err());
        assert!(SealHash::from_hex("zz").is_err());
    }

    #[test]
    fn a_changed_chunk_changes_the_hash() {
        let original = SealHash::of_chunks(["Hel", "lo"]).unwrap();
        let altered = SealHash::of_chunks(["Hel", "lp"]).unwrap();
        assert_ne!(original, altered);
    }
}
