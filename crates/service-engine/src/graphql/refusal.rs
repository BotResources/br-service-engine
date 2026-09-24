use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::graphql::CODE_EXTENSION;

pub const BODY_TOO_LARGE_CODE: &str = "BODY_TOO_LARGE";
pub const BODY_READ_TIMEOUT_CODE: &str = "BODY_READ_TIMEOUT";

pub(crate) fn coded_refusal(status: StatusCode, code: &'static str, message: &str) -> Response {
    (status, Json(coded_body(code, message))).into_response()
}

pub(crate) fn coded_body(code: &'static str, message: &str) -> serde_json::Value {
    let mut extensions = serde_json::Map::new();
    extensions.insert(CODE_EXTENSION.to_string(), code.into());
    serde_json::json!({
        "errors": [{ "message": message, "extensions": extensions }],
    })
}
