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

pub(crate) const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);

const COMPLETE: &[u8] = b"event: complete\ndata:\n\n";
const KEEP_ALIVE: &[u8] = b":\n\n";

pub(crate) fn wants_event_stream(headers: &HeaderMap) -> bool {
    headers
        .get_all(ACCEPT)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|range| range.split(';').next())
        .any(|media| media.trim().eq_ignore_ascii_case(EVENT_STREAM))
}

pub(crate) struct StreamBounds {
    pub(crate) max_age: Duration,
    pub(crate) keep_alive: Duration,
    pub(crate) shutdown: watch::Receiver<bool>,
}

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

fn next_frame(response: &async_graphql::Response) -> Bytes {
    let data = serde_json::to_string(response).unwrap_or_else(|error| {
        tracing::error!(
            error = %crate::chain::describe(&error),
            "serializing a graphql response for an event stream failed; the client receives INTERNAL"
        );
        coded_body(INTERNAL_CODE, INTERNAL_MESSAGE).to_string()
    });
    Bytes::from(format!("event: next\ndata: {data}\n\n"))
}

#[cfg(test)]
#[path = "sse_tests.rs"]
mod tests;
