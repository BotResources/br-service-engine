//! The refusals the engine answers itself on `POST /graphql` and on the WebSocket upgrade,
//! before the executor runs. They carry an HTTP status, and a GraphQL-shaped body with the
//! code in `errors[0].extensions.code`, so a client reads every coded refusal the same way.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::graphql::CODE_EXTENSION;

/// An authenticated body that is not multipart exceeds `MultipartConfig::max_body_bytes`,
/// by its declared `Content-Length` or as it streams in. HTTP 413.
pub const BODY_TOO_LARGE_CODE: &str = "BODY_TOO_LARGE";
/// An authenticated body, of any content type, did not arrive whole within
/// `EngineConfig::body_read_timeout`. HTTP 408.
pub const BODY_READ_TIMEOUT_CODE: &str = "BODY_READ_TIMEOUT";

/// `status` with `{"errors":[{"message": message, "extensions":{"code": code}}]}`. The
/// message names the violated bound, never a path, an OS error or a database error.
pub(crate) fn coded_refusal(status: StatusCode, code: &'static str, message: &str) -> Response {
    (status, Json(coded_body(code, message))).into_response()
}

/// `{"errors":[{"message": message, "extensions":{"code": code}}]}`: the GraphQL response
/// shape of a coded refusal, for a caller that frames it itself.
pub(crate) fn coded_body(code: &'static str, message: &str) -> serde_json::Value {
    let mut extensions = serde_json::Map::new();
    extensions.insert(CODE_EXTENSION.to_string(), code.into());
    serde_json::json!({
        "errors": [{ "message": message, "extensions": extensions }],
    })
}
