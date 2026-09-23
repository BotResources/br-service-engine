//! The GraphQL multipart request (`operations` + `map` + file parts) on `POST /graphql`.
//!
//! The router resolves the passport from the headers before this module reads a byte of
//! the body, so everything here runs for an authenticated request only. What it adds over
//! async-graphql's own parser is the bound: a body, part and upload-count ceiling checked
//! before anything is spooled, the spec's part order enforced so an unmapped or surplus
//! part is refused before it is spooled, and no spooling at all for a schema that declares
//! no `Upload`.

mod receive;
mod refusal;

#[cfg(test)]
mod tests;

use std::path::PathBuf;

use crate::config::{
    DEFAULT_MULTIPART_MAX_BODY_BYTES, DEFAULT_MULTIPART_MAX_FILE_BYTES, DEFAULT_MULTIPART_MAX_FILES,
};
use crate::error::EngineError;

pub(crate) use receive::receive;
pub(crate) use refusal::MultipartRefusal;
pub use refusal::{
    MULTIPART_FILE_TOO_LARGE_CODE, MULTIPART_MALFORMED_CODE, MULTIPART_SPOOL_UNAVAILABLE_CODE,
    MULTIPART_TOO_LARGE_CODE, MULTIPART_TOO_MANY_FILES_CODE,
};

/// The bounds of an authenticated multipart request, and where its file parts spool.
///
/// `max_body_bytes` caps the whole body (a declared `Content-Length` above it is refused
/// before the body is read; a chunked body is cut when it crosses it). `max_file_bytes` caps
/// any single part — a file, and the `operations` and `map` parts too. `max_files` caps the
/// uploads a request binds: every path in `map` counts, so one file bound to two variables
/// counts twice (each binding holds its own file handle). A schema that declares no `Upload`
/// scalar accepts no file whatever `max_files` says.
///
/// File parts spool to anonymous temporary files in `spool_dir`, or in
/// [`std::env::temp_dir`] (`$TMPDIR`, else `/tmp`) when it is `None`; the space is freed when
/// the request ends. Under a read-only root filesystem a service whose schema declares
/// `Upload` mounts a writable volume there; a spool it cannot write is refused with
/// [`MULTIPART_SPOOL_UNAVAILABLE_CODE`], never a partial request.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MultipartConfig {
    pub max_body_bytes: u64,
    pub max_file_bytes: u64,
    pub max_files: usize,
    pub spool_dir: Option<PathBuf>,
}

impl Default for MultipartConfig {
    fn default() -> Self {
        Self {
            max_body_bytes: DEFAULT_MULTIPART_MAX_BODY_BYTES,
            max_file_bytes: DEFAULT_MULTIPART_MAX_FILE_BYTES,
            max_files: DEFAULT_MULTIPART_MAX_FILES,
            spool_dir: None,
        }
    }
}

impl MultipartConfig {
    pub fn with_max_body_bytes(mut self, max_body_bytes: u64) -> Self {
        self.max_body_bytes = max_body_bytes;
        self
    }

    pub fn with_max_file_bytes(mut self, max_file_bytes: u64) -> Self {
        self.max_file_bytes = max_file_bytes;
        self
    }

    pub fn with_max_files(mut self, max_files: usize) -> Self {
        self.max_files = max_files;
        self
    }

    pub fn with_spool_dir(mut self, spool_dir: impl Into<PathBuf>) -> Self {
        self.spool_dir = Some(spool_dir.into());
        self
    }

    pub fn validate(&self) -> Result<(), EngineError> {
        if self.max_body_bytes == 0 || self.max_file_bytes == 0 {
            return Err(EngineError::Config(
                "multipart max_body_bytes and max_file_bytes must be non-zero, or no multipart \
                 request could carry even its `operations` part"
                    .into(),
            ));
        }
        if self.max_file_bytes > self.max_body_bytes {
            return Err(EngineError::Config(
                "multipart max_file_bytes must not exceed max_body_bytes: a part is part of the body"
                    .into(),
            ));
        }
        Ok(())
    }
}

/// The multipart bounds the router applies, fixed once when the app is built: the configured
/// limits, with `max_files` forced to zero when the schema declares no `Upload`.
#[derive(Debug, Clone)]
pub(crate) struct MultipartPolicy {
    max_body_bytes: u64,
    max_file_bytes: u64,
    max_files: usize,
    spool_dir: PathBuf,
}

impl MultipartPolicy {
    pub(crate) fn new(config: &MultipartConfig, schema_declares_upload: bool) -> Self {
        Self {
            max_body_bytes: config.max_body_bytes,
            max_file_bytes: config.max_file_bytes,
            max_files: if schema_declares_upload {
                config.max_files
            } else {
                0
            },
            spool_dir: config.spool_dir.clone().unwrap_or_else(std::env::temp_dir),
        }
    }

    pub(crate) fn max_body_bytes(&self) -> u64 {
        self.max_body_bytes
    }

    pub(crate) fn max_file_bytes(&self) -> u64 {
        self.max_file_bytes
    }

    pub(crate) fn max_files(&self) -> usize {
        self.max_files
    }

    pub(crate) fn spool_dir(&self) -> &std::path::Path {
        &self.spool_dir
    }
}
