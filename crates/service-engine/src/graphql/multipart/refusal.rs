use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::graphql::refusal::coded_refusal;

/// The multipart body exceeds `max_body_bytes` (declared or streamed). HTTP 413.
pub const MULTIPART_TOO_LARGE_CODE: &str = "MULTIPART_TOO_LARGE";
/// One part — a file, or the `operations` / `map` part — exceeds `max_file_bytes`. HTTP 413.
pub const MULTIPART_FILE_TOO_LARGE_CODE: &str = "MULTIPART_FILE_TOO_LARGE";
/// The `map` binds more uploads than `max_files` (zero for a schema without `Upload`), or
/// outweighs the 1 KiB per allowed upload it may carry. HTTP 413.
pub const MULTIPART_TOO_MANY_FILES_CODE: &str = "MULTIPART_TOO_MANY_FILES";
/// The body is not a GraphQL multipart request as the spec orders it. HTTP 400.
pub const MULTIPART_MALFORMED_CODE: &str = "MULTIPART_MALFORMED";
/// A file part could not be spooled (no writable spool directory, disk full). HTTP 500.
pub const MULTIPART_SPOOL_UNAVAILABLE_CODE: &str = "MULTIPART_SPOOL_UNAVAILABLE";

#[derive(Debug)]
pub(crate) enum MultipartRefusal {
    TooLarge { limit: u64 },
    FileTooLarge { limit: u64 },
    TooManyFiles { limit: usize },
    Malformed(&'static str),
    SpoolUnavailable(std::io::Error),
}

impl MultipartRefusal {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::TooLarge { .. } => MULTIPART_TOO_LARGE_CODE,
            Self::FileTooLarge { .. } => MULTIPART_FILE_TOO_LARGE_CODE,
            Self::TooManyFiles { .. } => MULTIPART_TOO_MANY_FILES_CODE,
            Self::Malformed(_) => MULTIPART_MALFORMED_CODE,
            Self::SpoolUnavailable(_) => MULTIPART_SPOOL_UNAVAILABLE_CODE,
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::TooLarge { .. } | Self::FileTooLarge { .. } | Self::TooManyFiles { .. } => {
                StatusCode::PAYLOAD_TOO_LARGE
            }
            Self::Malformed(_) => StatusCode::BAD_REQUEST,
            Self::SpoolUnavailable(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// The client-facing sentence: the violated bound, never a path, an OS error or the
    /// parser's internals.
    fn message(&self) -> String {
        match self {
            Self::TooLarge { limit } => {
                format!("the multipart request exceeds the {limit}-byte body limit")
            }
            Self::FileTooLarge { limit } => {
                format!("a part of the multipart request exceeds the {limit}-byte part limit")
            }
            Self::TooManyFiles { limit } => {
                format!("the multipart request binds more than {limit} uploads")
            }
            Self::Malformed(reason) => format!("malformed multipart request: {reason}"),
            Self::SpoolUnavailable(_) => "the uploaded file could not be received".to_string(),
        }
    }
}

impl From<multer::Error> for MultipartRefusal {
    fn from(error: multer::Error) -> Self {
        match error {
            multer::Error::StreamSizeExceeded { limit } => Self::TooLarge { limit },
            multer::Error::FieldSizeExceeded { limit, .. } => Self::FileTooLarge { limit },
            _ => Self::Malformed("the body is not a well-formed multipart/form-data stream"),
        }
    }
}

impl IntoResponse for MultipartRefusal {
    fn into_response(self) -> Response {
        let code = self.code();
        match &self {
            Self::SpoolUnavailable(error) => tracing::error!(
                code,
                %error,
                "a multipart file part could not be spooled (is the spool directory writable, \
                 with space and file handles to spare?)"
            ),
            _ => tracing::debug!(code, "a multipart request was refused"),
        }
        coded_refusal(self.status(), code, &self.message())
    }
}
