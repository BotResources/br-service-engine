use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_graphql::{EmptyMutation, Error, Object, Schema, Subscription};
use axum::body::{Body, BodyDataStream};
use axum::http::header::{ACCEPT, CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::Response;
use futures_util::{Stream, StreamExt};
use tokio::sync::watch;

use super::{StreamBounds, respond, wants_event_stream};

const LONG: Duration = Duration::from_secs(3600);
const WITHIN: Duration = Duration::from_secs(5);
const COMPLETE: &str = "event: complete\ndata:\n\n";

struct Query;

#[Object]
impl Query {
    async fn value(&self) -> i32 {
        7
    }
}

/// Set when the operation's stream is dropped: the response no longer holds it.
#[derive(Clone, Default)]
struct Released(Arc<AtomicBool>);

struct ReleaseOnDrop(Released);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.0.store(true, Ordering::SeqCst);
    }
}

struct Subscription;

#[Subscription]
impl Subscription {
    async fn ticks(&self, count: i32) -> impl Stream<Item = i32> {
        futures_util::stream::iter(0..count)
    }

    async fn silent(&self, ctx: &async_graphql::Context<'_>) -> impl Stream<Item = i32> {
        let guard = ctx.data_opt::<Released>().cloned().map(ReleaseOnDrop);
        futures_util::stream::pending::<i32>().map(move |tick| {
            let _held = &guard;
            tick
        })
    }

    async fn refused(&self) -> Result<futures_util::stream::Empty<i32>, Error> {
        Err(crate::graphql::coded_error("FORBIDDEN", "forbidden"))
    }
}

type TestSchema = Schema<Query, EmptyMutation, Subscription>;

fn schema() -> TestSchema {
    Schema::build(Query, EmptyMutation, Subscription).finish()
}

fn bounds(max_age: Duration, keep_alive: Duration) -> (StreamBounds, watch::Sender<bool>) {
    let (sender, shutdown) = watch::channel(false);
    let bounds = StreamBounds {
        max_age,
        keep_alive,
        shutdown,
    };
    (bounds, sender)
}

fn open(query: &str, bounds: StreamBounds) -> Response {
    respond(&schema(), async_graphql::Request::new(query), bounds)
}

/// The whole body of a stream that ends on its own, within [`WITHIN`].
async fn whole_body(response: Response) -> String {
    let bytes = tokio::time::timeout(
        WITHIN,
        axum::body::to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("the stream ends on its own")
    .expect("the body is readable");
    String::from_utf8(bytes.to_vec()).expect("the stream is UTF-8")
}

async fn next_chunk(body: &mut BodyDataStream) -> Option<String> {
    let chunk = tokio::time::timeout(WITHIN, body.next())
        .await
        .expect("the stream answers within the wait")?
        .expect("the body is readable");
    Some(String::from_utf8(chunk.to_vec()).expect("the stream is UTF-8"))
}

fn stream_of(response: Response) -> BodyDataStream {
    Body::into_data_stream(response.into_body())
}

#[tokio::test]
async fn a_subscription_answers_one_next_per_response_then_one_complete() {
    let (bounds, _shutdown) = bounds(LONG, LONG);
    let response = open("subscription { ticks(count: 2) }", bounds);
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[CONTENT_TYPE], "text/event-stream");
    assert_eq!(response.headers()[CACHE_CONTROL], "no-cache");
    assert_eq!(
        whole_body(response).await,
        "event: next\ndata: {\"data\":{\"ticks\":0}}\n\n\
         event: next\ndata: {\"data\":{\"ticks\":1}}\n\n\
         event: complete\ndata:\n\n"
    );
}

#[tokio::test]
async fn a_query_sent_for_an_event_stream_answers_one_next_then_complete() {
    let (bounds, _shutdown) = bounds(LONG, LONG);
    assert_eq!(
        whole_body(open("{ value }", bounds)).await,
        "event: next\ndata: {\"data\":{\"value\":7}}\n\nevent: complete\ndata:\n\n"
    );
}

#[tokio::test]
async fn a_refused_subscription_rides_a_next_frame_then_completes() {
    let (bounds, _shutdown) = bounds(LONG, LONG);
    let body = whole_body(open("subscription { refused }", bounds)).await;
    let (next, rest) = body
        .split_once("\n\n")
        .expect("the refusal is one frame before the complete");
    assert_eq!(rest, COMPLETE, "one complete follows the refusal: {body}");
    let data = next
        .strip_prefix("event: next\ndata: ")
        .expect("the refusal rides a next frame, not a transport error");
    let payload: serde_json::Value = serde_json::from_str(data).expect("the data is JSON");
    assert_eq!(
        payload["errors"][0]["extensions"]["code"],
        serde_json::json!("FORBIDDEN"),
        "the coded refusal reaches the client as the WebSocket would carry it: {payload}"
    );
}

#[tokio::test]
async fn a_silent_stream_sends_a_keep_alive_comment_each_interval() {
    let (bounds, _shutdown) = bounds(LONG, Duration::from_millis(20));
    let mut body = stream_of(open("subscription { silent }", bounds));
    for _ in 0..3 {
        assert_eq!(next_chunk(&mut body).await.as_deref(), Some(":\n\n"));
    }
}

#[tokio::test]
async fn a_stream_older_than_its_max_age_completes_and_ends() {
    let max_age = Duration::from_millis(150);
    let (bounds, _shutdown) = bounds(max_age, LONG);
    let opened = tokio::time::Instant::now();
    let body = whole_body(open("subscription { silent }", bounds)).await;
    assert_eq!(body, COMPLETE);
    assert!(
        opened.elapsed() >= max_age,
        "the stream is completed at its max age, not before"
    );
}

#[tokio::test]
async fn a_shutdown_completes_an_open_stream() {
    let (bounds, shutdown) = bounds(LONG, Duration::from_millis(20));
    let mut body = stream_of(open("subscription { silent }", bounds));
    assert_eq!(
        next_chunk(&mut body).await.as_deref(),
        Some(":\n\n"),
        "the stream is open"
    );
    shutdown.send(true).expect("the stream still listens");
    let mut rest = String::new();
    while let Some(chunk) = next_chunk(&mut body).await {
        rest.push_str(&chunk);
    }
    assert!(
        rest.ends_with(COMPLETE) && rest.trim_start_matches(":\n\n") == COMPLETE,
        "the shutdown ends the stream with one complete: {rest:?}"
    );
}

#[tokio::test]
async fn a_stream_opened_during_shutdown_completes_without_executing() {
    let (bounds, shutdown) = bounds(LONG, LONG);
    shutdown.send(true).expect("the receiver is alive");
    assert_eq!(
        whole_body(open("subscription { ticks(count: 2) }", bounds)).await,
        COMPLETE,
        "a service shutting down serves no new operation"
    );
}

#[tokio::test]
async fn a_dropped_response_releases_its_operation() {
    let released = Released::default();
    let (bounds, _shutdown) = bounds(LONG, Duration::from_millis(20));
    let request = async_graphql::Request::new("subscription { silent }").data(released.clone());
    let mut body = stream_of(respond(&schema(), request, bounds));
    assert_eq!(
        next_chunk(&mut body).await.as_deref(),
        Some(":\n\n"),
        "the operation runs"
    );
    assert!(!released.0.load(Ordering::SeqCst), "the stream holds it");
    drop(body);
    assert!(
        released.0.load(Ordering::SeqCst),
        "a client that goes away releases the operation, and with it its session"
    );
}

#[test]
fn only_an_explicit_event_stream_media_range_asks_for_a_stream() {
    let accepts = |values: &[&str]| {
        let mut headers = HeaderMap::new();
        for value in values {
            headers.append(
                ACCEPT,
                HeaderValue::from_str(value).expect("a header value"),
            );
        }
        wants_event_stream(&headers)
    };
    assert!(accepts(&["text/event-stream"]));
    assert!(accepts(&["application/json, text/event-stream;q=0.9"]));
    assert!(accepts(&["Text/Event-Stream"]));
    assert!(accepts(&["application/json", "text/event-stream"]));
    assert!(!accepts(&[]));
    assert!(!accepts(&["application/json"]));
    assert!(!accepts(&["*/*"]));
    assert!(!accepts(&["text/*"]));
    assert!(!accepts(&["application/graphql-response+json"]));
}
