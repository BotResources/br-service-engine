use std::time::Duration;

use reqwest::StatusCode;
use reqwest::header::CONTENT_LENGTH;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};

use crate::blobs::config::BlobConfig;
use crate::blobs::post_policy::{PostPolicyInput, presign_post};
use crate::blobs::{DownloadUrl, UploadUrl};
use crate::error::EngineError;

pub(crate) struct ObjectStore {
    bucket: Bucket,
    credentials: Credentials,
    http: reqwest::Client,
    endpoint: String,
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
        let endpoint = config
            .endpoint
            .parse()
            .map_err(|error| EngineError::Config(format!("object storage endpoint: {error}")))?;
        let bucket = Bucket::new(
            endpoint,
            UrlStyle::Path,
            config.bucket.clone(),
            config.region.clone(),
        )
        .map_err(|error| EngineError::Config(format!("object storage bucket: {error}")))?;
        let credentials = Credentials::new(&config.access_key, &config.secret_key);
        let http = reqwest::Client::builder()
            .build()
            .map_err(|error| EngineError::Blob(error.to_string()))?;
        Ok(Self {
            bucket,
            credentials,
            http,
            endpoint: config.endpoint.clone(),
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
    ) -> Result<UploadUrl, EngineError> {
        presign_post(PostPolicyInput {
            endpoint: &self.endpoint,
            bucket: &self.bucket_name,
            region: &self.region,
            access_key: &self.access_key,
            secret_key: &self.secret_key,
            object_key,
            content_type,
            max_bytes,
            ttl: self.upload_ttl,
            now: chrono::Utc::now(),
        })
    }

    pub(crate) fn presign_download(
        &self,
        object_key: &str,
        content_type: &str,
        file_name: &str,
    ) -> DownloadUrl {
        let mut action = self.bucket.get_object(Some(&self.credentials), object_key);
        action.query_mut().insert(
            "response-content-disposition",
            content_disposition(file_name),
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
            .bucket
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
                bucket: self.bucket.name().to_string(),
                status: status.as_u16(),
            })
        }
    }

    pub(crate) async fn head_size(&self, object_key: &str) -> Result<Option<u64>, EngineError> {
        let url = self
            .bucket
            .head_object(Some(&self.credentials), object_key)
            .sign(Duration::from_secs(60));
        let response = self
            .http
            .head(url)
            .send()
            .await
            .map_err(|error| EngineError::Blob(error.to_string()))?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if status.is_success() {
            let size = response
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| {
                    EngineError::Blob(format!(
                        "HEAD of object {object_key} carried no parseable Content-Length"
                    ))
                })?;
            return Ok(Some(size));
        }
        Err(EngineError::Blob(format!(
            "HEAD of object {object_key} answered {status}"
        )))
    }

    pub(crate) async fn delete_object(&self, object_key: &str) -> Result<(), EngineError> {
        let url = self
            .bucket
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

fn content_disposition(file_name: &str) -> String {
    let sanitized: String = file_name
        .chars()
        .filter(|c| !c.is_control())
        .map(|c| if c == '"' || c == '\\' { '_' } else { c })
        .collect();
    format!("attachment; filename=\"{sanitized}\"")
}

impl std::fmt::Debug for ObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStore")
            .field("bucket", &self.bucket.name())
            .finish_non_exhaustive()
    }
}
