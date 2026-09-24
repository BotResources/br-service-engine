//! The gateway's subscription transport: GraphQL over Server-Sent Events, the graphql-sse
//! "distinct connections" mode, on the same `POST /graphql` route as queries and mutations.
//!
//! The gateway reaches a subgraph for a subscription only as `POST /graphql` with
//! `Accept: text/event-stream`; it never opens a WebSocket on that leg. The request is
//! authenticated and its body read exactly like any other `POST /graphql` (the passport
//! first, then the bounded body); only the answer differs: a `text/event-stream` carrying
//! one `event: next` per GraphQL response and one `event: complete` at the end. Any
//! operation may be sent this way — a query or a mutation answers one `next`, then
//! `complete`, as graphql-sse specifies.
//!
//! The stream is executed by the same schema, so a subscription attaches the same engine
//! session as one opened on `/graphql/ws` (same `Reset`/`Upsert`/`Remove` wire, same
//! cohorts, same principal refresh), and it is bounded exactly like a WebSocket session:
//! at `session_max_age`, measured from the request, and when the engine shuts down, it
//! sends `complete` and ends. A client that goes away drops the response body, which drops
//! the operation and releases its session. While nothing is sent, a comment frame keeps the
//! connection alive every [`KEEP_ALIVE_INTERVAL`].

use std::convert::Infallible;
use std::pin::Pin;
use std::time::Duration;

use async_graphql::{ObjectType, Schema, SubscriptionType};
use axum::body::Body;
use axum::http::header::{ACCEPT, CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue};
use axum::response::Response;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::{BoxStream, Stream};
use tokio::sync::watch;
use tokio::time::{Instant, Sleep};

use crate::graphql::refusal::coded_body;
use crate::graphql::{INTERNAL_CODE, INTERNAL_MESSAGE};

const EVENT_STREAM: &str = "text/event-stream";

/// The longest a stream stays silent: past it, a comment frame is sent.
pub(crate) const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);

const COMPLETE: &[u8] = b"event: complete\ndata:\n\n";
const KEEP_ALIVE: &[u8] = b":\n\n";

/// Whether the client asks for an event stream: a media range of `Accept` names
/// `text/event-stream`, whatever its case and parameters. A wildcard does not.
pub(crate) fn wants_event_stream(headers: &HeaderMap) -> bool {
    headers
        .get_all(ACCEPT)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|range| range.split(';').next())
        .any(|media| media.trim().eq_ignore_ascii_case(EVENT_STREAM))
}

/// What bounds one stream: the same `session_max_age` and shutdown signal that bound a
/// WebSocket connection, and the keep-alive cadence.
pub(crate) struct StreamBounds {
    pub(crate) max_age: Duration,
    pub(crate) keep_alive: Duration,
    pub(crate) shutdown: watch::Receiver<bool>,
}

/// Answers `request` — already authenticated, its principal in its data — as a graphql-sse
/// stream under `bounds`.
pub(crate) fn respond<Q, M, S>(
    schema: &Schema<Q, M, S>,
    request: async_graphql::Request,
    bounds: StreamBounds,
) -> Response
where
    Q: ObjectType + 'static,
    M: ObjectType + 'static,
    S: SubscriptionType + 'static,
{
    let frames = frames(schema.execute_stream(request), bounds);
    let mut response = Response::new(Body::from_stream(frames));
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(EVENT_STREAM));
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
}

/// The body: the operation's responses as `next` frames, keep-alive comments while it is
/// silent, and one `complete` when it ends, expires or the engine shuts down.
fn frames(
    responses: BoxStream<'static, async_graphql::Response>,
    bounds: StreamBounds,
) -> impl Stream<Item = Result<Bytes, Infallible>> + Send + 'static {
    let open = Open {
        responses,
        deadline: Box::pin(tokio::time::sleep(bounds.max_age)),
        idle: Box::pin(tokio::time::sleep(bounds.keep_alive)),
        keep_alive: bounds.keep_alive,
        shutdown: bounds.shutdown,
    };
    futures_util::stream::unfold(Some(open), |open| async move {
        let mut open = open?;
        match open.advance().await {
            Frame::Next(frame) => Some((Ok(frame), Some(open))),
            Frame::KeepAlive => Some((Ok(Bytes::from_static(KEEP_ALIVE)), Some(open))),
            Frame::Complete => Some((Ok(Bytes::from_static(COMPLETE)), None)),
        }
    })
}

enum Frame {
    Next(Bytes),
    KeepAlive,
    Complete,
}

struct Open {
    responses: BoxStream<'static, async_graphql::Response>,
    deadline: Pin<Box<Sleep>>,
    idle: Pin<Box<Sleep>>,
    keep_alive: Duration,
    shutdown: watch::Receiver<bool>,
}

impl Open {
    async fn advance(&mut self) -> Frame {
        if *self.shutdown.borrow_and_update() {
            return Frame::Complete;
        }
        loop {
            tokio::select! {
                biased;
                () = &mut self.deadline => return Frame::Complete,
                changed = self.shutdown.changed() => {
                    if changed.is_err() || *self.shutdown.borrow() {
                        return Frame::Complete;
                    }
                }
                response = self.responses.next() => {
                    let Some(response) = response else {
                        return Frame::Complete;
                    };
                    self.rearm_idle();
                    return Frame::Next(next_frame(&response));
                }
                () = &mut self.idle => {
                    self.rearm_idle();
                    return Frame::KeepAlive;
                }
            }
        }
    }

    fn rearm_idle(&mut self) {
        self.idle.as_mut().reset(Instant::now() + self.keep_alive);
    }
}

/// `event: next` carrying the GraphQL response on one `data` line (compact JSON escapes
/// every newline). A response that cannot be serialized is logged and replaced by a coded
/// `INTERNAL` response, never by an empty frame.
fn next_frame(response: &async_graphql::Response) -> Bytes {
    let data = serde_json::to_string(response).unwrap_or_else(|error| {
        tracing::error!(
            %error,
            "serializing a graphql response for an event stream failed; the client receives INTERNAL"
        );
        coded_body(INTERNAL_CODE, INTERNAL_MESSAGE).to_string()
    });
    Bytes::from(format!("event: next\ndata: {data}\n\n"))
}

#[cfg(test)]
#[path = "sse_tests.rs"]
mod tests;
