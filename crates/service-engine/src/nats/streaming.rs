use serde::{Deserialize, Serialize};

use crate::error::EngineError;
use crate::wire::KeyBytes;

pub fn streaming_stream(service: &str) -> String {
    format!("STREAMING_{service}")
}

pub fn streaming_filter(service: &str) -> String {
    format!("stream.{service}.>")
}

pub fn chunk_subject(service: &str, token: &str) -> String {
    format!("stream.{service}.{token}")
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamFrame {
    pub accumulator: String,
    pub key: serde_json::Value,
    pub seq: u64,
    pub chunk: serde_json::Value,
}

impl StreamFrame {
    pub fn message_id(&self, token: &str) -> String {
        format!("{token}:{}", self.seq)
    }
}

pub fn subject_token(key: &serde_json::Value) -> Result<String, EngineError> {
    match key {
        serde_json::Value::String(text) if is_token_safe(text) => Ok(text.clone()),
        serde_json::Value::Number(number) => Ok(number.to_string()),
        serde_json::Value::Bool(flag) => Ok(flag.to_string()),
        other => {
            let bytes = KeyBytes::encode(other)?;
            Ok(format!("x{}", hex_lower(bytes.as_slice())))
        }
    }
}

fn is_token_safe(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= 200
        && text
            .chars()
            .all(|c| !c.is_whitespace() && !matches!(c, '.' | '*' | '>' | '\0'))
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from_digit((byte >> 4) as u32, 16).unwrap());
        out.push(char::from_digit((byte & 0x0f) as u32, 16).unwrap());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_uuid_string_key_is_its_own_readable_subject_token() {
        let id = "550e8400-e29b-41d4-a716-446655440000";
        let token = subject_token(&serde_json::json!(id)).unwrap();
        assert_eq!(token, id);
        assert_eq!(chunk_subject("chat", &token), format!("stream.chat.{id}"));
    }

    #[test]
    fn a_structured_or_unsafe_key_falls_back_to_a_deterministic_hex_token() {
        let structured = serde_json::json!({ "board": 7, "line": "a.b" });
        let one = subject_token(&structured).unwrap();
        let two = subject_token(&structured).unwrap();
        assert_eq!(one, two, "the token is deterministic");
        assert!(one.starts_with('x'));
        assert!(is_token_safe(&one), "the fallback token is a legal subject");

        let dotted = subject_token(&serde_json::json!("a.b*c")).unwrap();
        assert!(dotted.starts_with('x'), "an unsafe string key is escaped");
    }

    #[test]
    fn the_stream_and_filter_follow_the_service_label() {
        assert_eq!(streaming_stream("chat"), "STREAMING_chat");
        assert_eq!(streaming_filter("chat"), "stream.chat.>");
    }
}
