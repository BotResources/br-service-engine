use std::time::Duration;

use async_graphql::ParseRequestError;
use async_graphql::http::MultipartOptions;
use async_graphql_axum::rejection::GraphQLRejection;
use axum::body::Body;
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::{Stream, StreamExt, TryStreamExt};

use crate::graphql::multipart::{self, MultipartPolicy, MultipartRefusal};
use crate::graphql::refusal::{BODY_READ_TIMEOUT_CODE, BODY_TOO_LARGE_CODE, coded_refusal};

/// How an authenticated `POST /graphql` body is read: the multipart bounds, whose
/// `max_body_bytes` caps every body whatever its content type, and the deadline the whole
/// body must arrive within.
#[derive(Debug, Clone)]
pub(crate) struct BodyPolicy {
    multipart: MultipartPolicy,
    read_timeout: Duration,
}

impl BodyPolicy {
    pub(crate) fn new(multipart: MultipartPolicy, read_timeout: Duration) -> Self {
        Self {
            multipart,
            read_timeout,
        }
    }

    pub(crate) fn multipart(&self) -> &MultipartPolicy {
        &self.multipart
    }

    pub(crate) fn read_timeout(&self) -> Duration {
        self.read_timeout
    }
}

/// Why a body yields no GraphQL request: a bound the engine enforces, a multipart shape it
/// refuses, or a parse error async-graphql reports, rendered exactly as its axum extractor
/// renders it.
pub(crate) enum BodyRefusal {
    TooLarge { limit: u64 },
    ReadTimeout { after: Duration },
    Multipart(MultipartRefusal),
    Parse(GraphQLRejection),
}

impl IntoResponse for BodyRefusal {
    fn into_response(self) -> Response {
        match self {
            Self::TooLarge { limit } => {
                tracing::debug!(
                    code = BODY_TOO_LARGE_CODE,
                    limit,
                    "a request body was refused"
                );
                coded_refusal(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    BODY_TOO_LARGE_CODE,
                    &format!("the request body exceeds the {limit}-byte body limit"),
                )
            }
            Self::ReadTimeout { after } => {
                let after_ms = after.as_millis();
                tracing::debug!(
                    code = BODY_READ_TIMEOUT_CODE,
                    after_ms = %after_ms,
                    "a request body was refused"
                );
                coded_refusal(
                    StatusCode::REQUEST_TIMEOUT,
                    BODY_READ_TIMEOUT_CODE,
                    &format!("the request body did not arrive within {after_ms} ms"),
                )
            }
            Self::Multipart(refusal) => refusal.into_response(),
            Self::Parse(rejection) => rejection.into_response(),
        }
    }
}

/// Reads the GraphQL request out of an authenticated `POST /graphql` body, under `policy`.
///
/// The whole read — every content type — must finish within `policy.read_timeout()`, or it
/// is abandoned and whatever was received (buffered bytes, spooled files) is dropped with
/// it. A `multipart/*` body goes through the engine's bounded receiver. Every other body is
/// bounded by `max_body_bytes` (a declared `Content-Length` above it is refused unread, a
/// chunked body is cut as it crosses it) and handed, as a lazily read stream, to the
/// function async-graphql-axum's `GraphQLRequest` extractor calls — same content-type
/// dispatch (an unparseable type is refused before the body is read), same single-request
/// rule, same rejection — so a JSON client within the bounds sees no change.
pub(crate) async fn receive(
    headers: &HeaderMap,
    body: Body,
    policy: &BodyPolicy,
) -> Result<async_graphql::Request, BodyRefusal> {
    let after = policy.read_timeout();
    tokio::time::timeout(after, read(headers, body, policy.multipart()))
        .await
        .unwrap_or(Err(BodyRefusal::ReadTimeout { after }))
}

async fn read(
    headers: &HeaderMap,
    body: Body,
    policy: &MultipartPolicy,
) -> Result<async_graphql::Request, BodyRefusal> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    let declared_length = headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    if let Some(content_type) = content_type.filter(|value| is_multipart(value)) {
        return multipart::receive(
            content_type,
            declared_length,
            body.into_data_stream(),
            policy,
        )
        .await
        .map_err(BodyRefusal::Multipart);
    }
    let limit = policy.max_body_bytes();
    if declared_length.is_some_and(|length| length > limit) {
        return Err(BodyRefusal::TooLarge { limit });
    }
    let reader = bounded(body, limit).into_async_read();
    async_graphql::http::receive_body(content_type, reader, MultipartOptions::default())
        .await
        .map_err(|error| refusal(error, limit))
}

/// The body as a stream of chunks that fails with [`BodyLimitExceeded`] on the chunk that
/// takes it past `limit`: nothing after that chunk is pulled from the connection.
fn bounded(body: Body, limit: u64) -> impl Stream<Item = std::io::Result<Bytes>> + Unpin {
    let mut received: u64 = 0;
    body.into_data_stream().map(move |chunk| {
        let chunk = chunk.map_err(|error| std::io::Error::other(error.to_string()))?;
        received = received.saturating_add(chunk.len() as u64);
        if received > limit {
            return Err(std::io::Error::other(BodyLimitExceeded));
        }
        Ok(chunk)
    })
}

#[derive(Debug)]
struct BodyLimitExceeded;

impl std::fmt::Display for BodyLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the request body crossed its limit")
    }
}

impl std::error::Error for BodyLimitExceeded {}

/// The same test async-graphql applies before it hands a body to its own multipart parser,
/// so no multipart body can reach that unbounded path.
fn is_multipart(content_type: &str) -> bool {
    content_type
        .parse::<mime::Mime>()
        .is_ok_and(|mime| mime.type_() == mime::MULTIPART)
}

/// async-graphql reads the body with `read_to_end` and hands its I/O error back unchanged,
/// so the limit's own error is recognised under [`ParseRequestError::Io`].
fn refusal(error: ParseRequestError, limit: u64) -> BodyRefusal {
    match error {
        ParseRequestError::Io(io)
            if io
                .get_ref()
                .is_some_and(|inner| inner.is::<BodyLimitExceeded>()) =>
        {
            BodyRefusal::TooLarge { limit }
        }
        error => BodyRefusal::Parse(GraphQLRejection(error)),
    }
}

#[cfg(test)]
#[path = "body_tests.rs"]
mod tests;
