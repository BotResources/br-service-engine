use std::time::Duration;

use base64::prelude::{BASE64_STANDARD, Engine as _};
use reqwest::StatusCode;
use reqwest::header::{CONTENT_LENGTH, ETAG};
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};

use crate::blobs::config::BlobConfig;
use crate::blobs::expect::Sha256Digest;
use crate::blobs::post_policy::{PostPolicyInput, presign_post};
use crate::blobs::{Disposition, DownloadUrl, UploadUrl};
use crate::error::EngineError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectHead {
    pub size: u64,
    pub etag: String,
    pub sha256: Option<Sha256Digest>,
}

pub(crate) struct ObjectStore {
    internal: Bucket,
    public: Bucket,
    credentials: Credentials,
    http: reqwest::Client,
    public_endpoint: String,
    bucket_name: String,
    region: String,
    access_key: String,
    secret_key: String,
    upload_ttl: Duration,
    download_ttl: Duration,
}

impl ObjectStore {
    pub(crate) fn from_config(config: &BlobConfig) -> Result<Self, EngineError> {
        config.validate()?;
        let internal = bucket(&config.endpoint, &config.bucket, &config.region)?;
        let public = match &config.public_endpoint {
            Some(endpoint) => bucket(endpoint, &config.bucket, &config.region)?,
            None => internal.clone(),
        };
        let public_endpoint = config
            .public_endpoint
            .clone()
            .unwrap_or_else(|| config.endpoint.clone());
        let credentials = Credentials::new(&config.access_key, &config.secret_key);
        let http = reqwest::Client::builder()
            .build()
            .map_err(|error| EngineError::Blob(error.to_string()))?;
        Ok(Self {
            internal,
            public,
            credentials,
            http,
            public_endpoint,
            bucket_name: config.bucket.clone(),
            region: config.region.clone(),
            access_key: config.access_key.clone(),
            secret_key: config.secret_key.clone(),
            upload_ttl: config.upload_ttl,
            download_ttl: config.download_ttl,
        })
    }

    pub(crate) fn presign_upload(
        &self,
        object_key: &str,
        max_bytes: u64,
        content_type: &str,
        expect: Option<(u64, &Sha256Digest)>,
    ) -> Result<UploadUrl, EngineError> {
        presign_post(PostPolicyInput {
            endpoint: &self.public_endpoint,
            bucket: &self.bucket_name,
            region: &self.region,
            access_key: &self.access_key,
            secret_key: &self.secret_key,
            object_key,
            content_type,
            max_bytes,
            expect,
            ttl: self.upload_ttl,
            now: chrono::Utc::now(),
        })
    }

    pub(crate) fn presign_download(
        &self,
        object_key: &str,
        content_type: &str,
        file_name: &str,
        disposition: Disposition,
    ) -> DownloadUrl {
        let mut action = self.public.get_object(Some(&self.credentials), object_key);
        action.query_mut().insert(
            "response-content-disposition",
            content_disposition(file_name, disposition),
        );
        if !content_type.is_empty() {
            action
                .query_mut()
                .insert("response-content-type", content_type.to_string());
        }
        let url = action.sign(self.download_ttl);
        DownloadUrl::new(url.to_string())
    }

    pub(crate) async fn ensure_bucket(&self) -> Result<(), EngineError> {
        let url = self
            .internal
            .head_bucket(Some(&self.credentials))
            .sign(Duration::from_secs(60));
        let status = self
            .http
            .head(url)
            .send()
            .await
            .map_err(|error| EngineError::Blob(error.to_string()))?
            .status();
        if status.is_success() {
            Ok(())
        } else {
            Err(EngineError::BlobBucketAbsent {
                bucket: self.internal.name().to_string(),
                status: status.as_u16(),
            })
        }
    }

    pub(crate) async fn head(&self, object_key: &str) -> Result<Option<ObjectHead>, EngineError> {
        let url = self
            .internal
            .head_object(Some(&self.credentials), object_key)
            .sign(Duration::from_secs(60));
        let response = self
            .http
            .head(url)
            .header("x-amz-checksum-mode", "ENABLED")
            .send()
            .await
            .map_err(|error| EngineError::Blob(error.to_string()))?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(EngineError::Blob(format!(
                "HEAD of object {object_key} answered {status}"
            )));
        }
        let headers = response.headers();
        let size = headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| {
                EngineError::Blob(format!(
                    "HEAD of object {object_key} carried no parseable Content-Length"
                ))
            })?;
        let etag = headers
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.trim_matches('"').to_string())
            .unwrap_or_default();
        let sha256 = headers
            .get("x-amz-checksum-sha256")
            .and_then(|value| value.to_str().ok())
            .and_then(decode_checksum);
        Ok(Some(ObjectHead { size, etag, sha256 }))
    }

    pub(crate) async fn delete_object(&self, object_key: &str) -> Result<(), EngineError> {
        let url = self
            .internal
            .delete_object(Some(&self.credentials), object_key)
            .sign(Duration::from_secs(60));
        let status = self
            .http
            .delete(url)
            .send()
            .await
            .map_err(|error| EngineError::Blob(error.to_string()))?
            .status();
        if status.is_success() || status == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(EngineError::Blob(format!(
                "DELETE of object {object_key} answered {status}"
            )))
        }
    }
}

fn bucket(endpoint: &str, name: &str, region: &str) -> Result<Bucket, EngineError> {
    let parsed = endpoint
        .parse()
        .map_err(|error| EngineError::Config(format!("object storage endpoint: {error}")))?;
    Bucket::new(parsed, UrlStyle::Path, name.to_string(), region.to_string())
        .map_err(|error| EngineError::Config(format!("object storage bucket: {error}")))
}

fn decode_checksum(value: &str) -> Option<Sha256Digest> {
    let bytes = BASE64_STANDARD.decode(value.trim()).ok()?;
    let array: [u8; 32] = bytes.try_into().ok()?;
    Some(Sha256Digest::from_bytes(array))
}

fn content_disposition(file_name: &str, disposition: Disposition) -> String {
    let clean: String = file_name.chars().filter(|c| !c.is_control()).collect();
    let ascii: String = clean
        .chars()
        .map(|c| {
            if !c.is_ascii() || c == '"' || c == '\\' {
                '_'
            } else {
                c
            }
        })
        .collect();
    let encoded = rfc5987_encode(&clean);
    format!(
        "{}; filename=\"{ascii}\"; filename*=UTF-8''{encoded}",
        disposition.keyword()
    )
}

fn rfc5987_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for &byte in value.as_bytes() {
        let attr_char = byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'!' | b'#' | b'$' | b'&' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~'
            );
        if attr_char {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push_str(&format!("{byte:02X}"));
        }
    }
    out
}

impl std::fmt::Debug for ObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStore")
            .field("bucket", &self.internal.name())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_disposition_renders_the_requested_keyword() {
        assert_eq!(
            content_disposition("report.pdf", Disposition::Attachment),
            "attachment; filename=\"report.pdf\"; filename*=UTF-8''report.pdf"
        );
        assert_eq!(
            content_disposition("report.pdf", Disposition::Inline),
            "inline; filename=\"report.pdf\"; filename*=UTF-8''report.pdf"
        );
    }

    #[test]
    fn a_quote_newline_or_backslash_cannot_break_out_of_the_header() {
        let out = content_disposition("a\"b\\c\nd\re", Disposition::Attachment);
        assert_eq!(
            out,
            "attachment; filename=\"a_b_cde\"; filename*=UTF-8''a%22b%5Ccde"
        );
        assert!(!out.contains('\n') && !out.contains('\r'));
    }

    #[test]
    fn a_non_ascii_name_falls_back_to_ascii_and_is_percent_encoded() {
        let out = content_disposition("rapport été.pdf", Disposition::Attachment);
        assert_eq!(
            out,
            "attachment; filename=\"rapport _t_.pdf\"; \
             filename*=UTF-8''rapport%20%C3%A9t%C3%A9.pdf"
        );
        assert!(out.is_ascii());
    }
}
