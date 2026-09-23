use base64::prelude::{BASE64_STANDARD, Engine as _};
use serde::{Serialize, Serializer};

use crate::error::EngineError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_hex(hex: &str) -> Result<Self, EngineError> {
        let trimmed = hex.trim();
        if trimmed.len() != 64 {
            return Err(invalid(hex));
        }
        let bytes = trimmed.as_bytes();
        let mut out = [0u8; 32];
        for (index, byte) in out.iter_mut().enumerate() {
            let hi = nibble(bytes[index * 2]).ok_or_else(|| invalid(hex))?;
            let lo = nibble(bytes[index * 2 + 1]).ok_or_else(|| invalid(hex))?;
            *byte = (hi << 4) | lo;
        }
        Ok(Self(out))
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        crate::hex::lower(&self.0)
    }

    pub fn to_base64(&self) -> String {
        BASE64_STANDARD.encode(self.0)
    }
}

impl Serialize for Sha256Digest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn invalid(value: &str) -> EngineError {
    EngineError::InvalidName {
        kind: "sha256 hex digest",
        value: value.to_string(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UploadExpectation {
    pub size: u64,
    pub sha256: Sha256Digest,
}

impl UploadExpectation {
    pub fn new(size: u64, sha256: Sha256Digest) -> Self {
        Self { size, sha256 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_empty_string_digest_round_trips_hex_and_base64() {
        let digest = Sha256Digest::from_hex(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .expect("a 64-char hex digest parses");
        assert_eq!(
            digest.to_hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            digest.to_base64(),
            "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
        );
    }

    #[test]
    fn an_uppercase_hex_digest_parses_to_the_same_bytes() {
        let lower = Sha256Digest::from_hex(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();
        let upper = Sha256Digest::from_hex(
            "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855",
        )
        .unwrap();
        assert_eq!(lower, upper);
    }

    #[test]
    fn a_short_or_non_hex_digest_fails_loud() {
        assert!(Sha256Digest::from_hex("abc").is_err());
        assert!(Sha256Digest::from_hex(&"z".repeat(64)).is_err());
    }

    #[test]
    fn the_digest_serializes_as_its_lowercase_hex() {
        let digest = Sha256Digest::from_bytes([0xab; 32]);
        let json = serde_json::to_string(&digest).unwrap();
        assert_eq!(json, format!("\"{}\"", "ab".repeat(32)));
    }
}
