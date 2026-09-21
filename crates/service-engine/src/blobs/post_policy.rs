use std::fmt::Write as _;
use std::time::Duration;

use base64::prelude::{BASE64_STANDARD, Engine as _};
use chrono::{DateTime, SecondsFormat, Utc};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::json;
use sha2::Sha256;

use crate::blobs::UploadUrl;
use crate::blobs::expect::Sha256Digest;
use crate::error::EngineError;

type HmacSha256 = Hmac<Sha256>;

pub(crate) struct PostPolicyInput<'a> {
    pub endpoint: &'a str,
    pub bucket: &'a str,
    pub region: &'a str,
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub object_key: &'a str,
    pub content_type: &'a str,
    pub max_bytes: u64,
    pub expect: Option<(u64, &'a Sha256Digest)>,
    pub ttl: Duration,
    pub now: DateTime<Utc>,
}

pub(crate) fn presign_post(input: PostPolicyInput<'_>) -> Result<UploadUrl, EngineError> {
    let amz_date = input.now.format("%Y%m%dT%H%M%SZ").to_string();
    let yyyymmdd = input.now.format("%Y%m%d").to_string();
    let ttl = chrono::Duration::from_std(input.ttl).map_err(|_| {
        EngineError::Config(format!(
            "the blob upload TTL {:?} does not fit a signed POST-policy expiry",
            input.ttl
        ))
    })?;
    let expiration = (input.now + ttl).to_rfc3339_opts(SecondsFormat::Millis, true);
    let credential = format!(
        "{}/{}/{}/s3/aws4_request",
        input.access_key, yyyymmdd, input.region
    );
    let max_bytes = i64::try_from(input.max_bytes).unwrap_or(i64::MAX);

    let mut conditions = vec![
        json!({ "bucket": input.bucket }),
        json!({ "key": input.object_key }),
        json!({ "Content-Type": input.content_type }),
        json!({ "x-amz-algorithm": "AWS4-HMAC-SHA256" }),
        json!({ "x-amz-credential": credential }),
        json!({ "x-amz-date": amz_date }),
    ];
    let checksum_b64 = input.expect.map(|(_, digest)| digest.to_base64());
    match (&input.expect, &checksum_b64) {
        (Some((size, _)), Some(checksum)) => {
            let size = i64::try_from(*size).unwrap_or(i64::MAX);
            conditions.push(json!({ "x-amz-checksum-algorithm": "SHA256" }));
            conditions.push(json!({ "x-amz-checksum-sha256": checksum }));
            conditions.push(json!(["content-length-range", size, size]));
        }
        _ => conditions.push(json!(["content-length-range", 0, max_bytes])),
    }

    let policy = json!({
        "expiration": expiration,
        "conditions": conditions,
    });
    let policy = serde_json::to_vec(&policy).map_err(|source| EngineError::Encode {
        what: "blob upload post policy",
        source,
    })?;
    let policy_b64 = BASE64_STANDARD.encode(&policy);
    let signature = sign(
        input.secret_key,
        &yyyymmdd,
        input.region,
        policy_b64.as_bytes(),
    )?;

    let url = format!("{}/{}", input.endpoint.trim_end_matches('/'), input.bucket);
    let mut fields = vec![
        ("key".to_string(), input.object_key.to_string()),
        ("Content-Type".to_string(), input.content_type.to_string()),
        (
            "x-amz-algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        ),
        ("x-amz-credential".to_string(), credential),
        ("x-amz-date".to_string(), amz_date),
    ];
    if let Some(checksum) = checksum_b64 {
        fields.push(("x-amz-checksum-algorithm".to_string(), "SHA256".to_string()));
        fields.push(("x-amz-checksum-sha256".to_string(), checksum));
    }
    fields.push(("policy".to_string(), policy_b64));
    fields.push(("x-amz-signature".to_string(), signature));
    Ok(UploadUrl::new(url, fields))
}

fn sign(secret: &str, date: &str, region: &str, payload: &[u8]) -> Result<String, EngineError> {
    let date_key = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes())?;
    let region_key = hmac(&date_key, region.as_bytes())?;
    let service_key = hmac(&region_key, b"s3")?;
    let signing_key = hmac(&service_key, b"aws4_request")?;
    let signature = hmac(&signing_key, payload)?;
    Ok(hex_lower(&signature))
}

fn hmac(key: &[u8], data: &[u8]) -> Result<Vec<u8>, EngineError> {
    let mut mac = HmacSha256::new_from_slice(key)
        .map_err(|_| EngineError::Blob("the SigV4 HMAC key length was rejected".into()))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(ttl: Duration) -> PostPolicyInput<'static> {
        PostPolicyInput {
            endpoint: "https://s3.example",
            bucket: "blobs",
            region: "eu-west",
            access_key: "AKIA",
            secret_key: "secret",
            object_key: "svc/card/abc",
            content_type: "image/png",
            max_bytes: 1024,
            expect: None,
            ttl,
            now: Utc::now(),
        }
    }

    fn conditions(url: &UploadUrl) -> Vec<serde_json::Value> {
        decoded_policy(url)["conditions"]
            .as_array()
            .expect("conditions array")
            .clone()
    }

    fn has_field(url: &UploadUrl, name: &str) -> bool {
        url.fields().iter().any(|(field, _)| field == name)
    }

    fn decoded_policy(url: &UploadUrl) -> serde_json::Value {
        let (_, field) = url
            .fields()
            .iter()
            .find(|(name, _)| name == "policy")
            .expect("the form carries the base64 policy");
        let bytes = BASE64_STANDARD.decode(field).expect("the policy is base64");
        serde_json::from_slice(&bytes).expect("the policy is json")
    }

    #[test]
    fn the_policy_pins_the_recorded_content_type() {
        let url = input(Duration::from_secs(900)).pipe_presign();
        let policy = decoded_policy(&url);
        let conditions = policy["conditions"].as_array().expect("conditions array");
        assert!(
            conditions
                .iter()
                .any(|c| c.get("Content-Type").and_then(|v| v.as_str()) == Some("image/png")),
            "the POST policy must condition the recorded Content-Type so the stored type is \
             enforced at upload"
        );
        assert!(
            url.fields()
                .iter()
                .any(|(name, value)| name == "Content-Type" && value == "image/png"),
            "the form must carry the Content-Type field the policy conditions"
        );
    }

    #[test]
    fn an_unrepresentable_ttl_fails_loud_instead_of_substituting_a_default() {
        let error = presign_post(input(Duration::from_secs(u64::MAX)))
            .expect_err("a TTL that does not fit a signed expiry must fail loud");
        assert!(matches!(error, EngineError::Config(_)));
    }

    #[test]
    fn an_unverified_policy_carries_the_open_content_length_range_and_no_checksum() {
        let url = input(Duration::from_secs(900)).pipe_presign();
        let conds = conditions(&url);
        assert!(conds.iter().any(|c| c.as_array().is_some_and(
            |a| a.first().and_then(|v| v.as_str()) == Some("content-length-range")
                && a.get(1).and_then(|v| v.as_i64()) == Some(0)
        )));
        assert!(!has_field(&url, "x-amz-checksum-sha256"));
    }

    #[test]
    fn a_verified_policy_pins_the_exact_size_and_both_checksum_conditions() {
        let digest = Sha256Digest::from_bytes([0x11; 32]);
        let mut input = input(Duration::from_secs(900));
        input.expect = Some((4096, &digest));
        let url = presign_post(input).expect("a verified policy signs");
        let conds = conditions(&url);
        assert!(
            conds.iter().any(|c| c.as_array().is_some_and(|a| {
                a.first().and_then(|v| v.as_str()) == Some("content-length-range")
                    && a.get(1).and_then(|v| v.as_i64()) == Some(4096)
                    && a.get(2).and_then(|v| v.as_i64()) == Some(4096)
            })),
            "the range must pin the exact expected byte count on both bounds"
        );
        assert!(
            conds.iter().any(
                |c| c.get("x-amz-checksum-algorithm").and_then(|v| v.as_str()) == Some("SHA256")
            )
        );
        assert!(
            conds
                .iter()
                .any(|c| c.get("x-amz-checksum-sha256").and_then(|v| v.as_str())
                    == Some(digest.to_base64().as_str()))
        );
        assert!(has_field(&url, "x-amz-checksum-algorithm"));
        assert!(has_field(&url, "x-amz-checksum-sha256"));
    }

    impl PostPolicyInput<'_> {
        fn pipe_presign(self) -> UploadUrl {
            presign_post(self).expect("a representable TTL signs a policy")
        }
    }
}
