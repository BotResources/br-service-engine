mod receive;
mod refusal;

#[cfg(test)]
mod config_tests;
#[cfg(test)]
mod tests;

use std::path::PathBuf;

use crate::config::{DEFAULT_MULTIPART_MAX_FILE_BYTES, DEFAULT_MULTIPART_MAX_FILES};

pub(crate) use receive::receive;
pub(crate) use refusal::MultipartRefusal;
pub use refusal::{
    MULTIPART_FILE_TOO_LARGE_CODE, MULTIPART_MALFORMED_CODE, MULTIPART_SPOOL_UNAVAILABLE_CODE,
    MULTIPART_TOO_LARGE_CODE, MULTIPART_TOO_MANY_FILES_CODE,
};

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MultipartConfig {
    pub max_file_bytes: u64,
    pub max_files: usize,
    pub spool_dir: Option<PathBuf>,
}

impl Default for MultipartConfig {
    fn default() -> Self {
        Self {
            max_file_bytes: DEFAULT_MULTIPART_MAX_FILE_BYTES,
            max_files: DEFAULT_MULTIPART_MAX_FILES,
            spool_dir: None,
        }
    }
}

impl MultipartConfig {
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
}

const UPLOAD_SCALAR: &str = "Upload";

pub(crate) fn declares_upload_scalar(sdl: &str) -> bool {
    sdl.lines().any(|line| {
        let mut words = line.split_whitespace();
        words.next() == Some("scalar") && words.next() == Some(UPLOAD_SCALAR)
    })
}

#[derive(Debug, Clone)]
pub(crate) struct MultipartPolicy {
    max_file_bytes: u64,
    max_files: usize,
    spool_dir: PathBuf,
}

impl MultipartPolicy {
    pub(crate) fn new(config: &MultipartConfig, schema_declares_upload: bool) -> Self {
        Self {
            max_file_bytes: config.max_file_bytes,
            max_files: if schema_declares_upload {
                config.max_files
            } else {
                0
            },
            spool_dir: config.spool_dir.clone().unwrap_or_else(std::env::temp_dir),
        }
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
