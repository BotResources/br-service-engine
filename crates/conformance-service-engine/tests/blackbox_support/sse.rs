//! The gateway's subscription leg read frame by frame: `POST /graphql` with
//! `Accept: text/event-stream`, answered in graphql-sse "distinct connections" framing. The
//! harness's `SseSubscription` hands back the data of `next` frames; this reader also sees the
//! `complete` frame, the keep-alive comments and the end of the body, which the gateway acts on.

use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub const EVENT_STREAM: &str = "text/event-stream";

/// A graphql-sse frame: a `next` carrying its GraphQL response, the `complete`, or a
/// keep-alive comment.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame {
    Next(serde_json::Value),
    Complete,
    Comment,
}

pub struct SseFrames {
    pub status: u16,
    pub content_type: String,
    pub cache_control: String,
    body: Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>,
    buffer: String,
}

impl SseFrames {
    /// Opens `query` on the gateway's transport, with `passport` in `X-Passport` when given.
    pub async fn open(base_url: &str, passport: Option<&str>, query: &str) -> Self {
        let mut request = reqwest::Client::new()
            .post(format!("{base_url}/graphql"))
            .header("accept", EVENT_STREAM)
            .json(&serde_json::json!({ "query": query }));
        if let Some(passport) = passport {
            request = request.header("x-passport", passport);
        }
        let response = request
            .send()
            .await
            .expect("the event-stream POST reaches the server");
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string()
        };
        Self {
            status: response.status().as_u16(),
            content_type: header("content-type"),
            cache_control: header("cache-control"),
            buffer: String::new(),
            body: Box::pin(response.bytes_stream()),
        }
    }

    /// The next whole frame, `None` once the body has ended. Panics if nothing arrives
    /// `within`, or on a block that is not graphql-sse.
    pub async fn next_frame(&mut self, within: Duration) -> Option<Frame> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            if let Some(end) = self.buffer.find("\n\n") {
                let block: String = self.buffer.drain(..end + 2).collect();
                return Some(parse(block.trim_end_matches('\n')));
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, self.body.next()).await {
                Err(_) => panic!(
                    "no whole frame within {within:?}; pending: {:?}",
                    self.buffer
                ),
                Ok(None) => {
                    assert!(
                        self.buffer.is_empty(),
                        "the body ended inside a frame: {:?}",
                        self.buffer
                    );
                    return None;
                }
                Ok(Some(chunk)) => {
                    let chunk = chunk.expect("the event stream is readable");
                    self.buffer
                        .push_str(std::str::from_utf8(&chunk).expect("UTF-8"));
                }
            }
        }
    }

    /// The next frame that is not a keep-alive comment.
    pub async fn next_event(&mut self, within: Duration) -> Option<Frame> {
        loop {
            match self.next_frame(within).await {
                Some(Frame::Comment) => continue,
                other => return other,
            }
        }
    }
}

fn parse(block: &str) -> Frame {
    if block.lines().all(|line| line.starts_with(':')) {
        return Frame::Comment;
    }
    let mut lines = block.lines();
    let event = lines.next().and_then(|line| line.strip_prefix("event: "));
    let data = lines.next().and_then(|line| line.strip_prefix("data:"));
    assert!(
        lines.next().is_none(),
        "a graphql-sse frame is one event line and one data line: {block:?}"
    );
    match (event, data) {
        (Some("next"), Some(data)) => {
            Frame::Next(serde_json::from_str(data.trim_start()).expect("a next frame carries JSON"))
        }
        (Some("complete"), Some("")) => Frame::Complete,
        _ => panic!("not a graphql-sse frame: {block:?}"),
    }
}

/// Sends an event-stream `POST /graphql` whose body stops after its first bytes, with a
/// `Content-Length` promising a kibibyte more that never comes, and waits `wait` for the
/// status line. `None`: the pod is still waiting for the body, so it reads before it judges.
pub async fn stalled_request(
    base_url: &str,
    passport: Option<&str>,
    wait: Duration,
) -> Option<String> {
    let addr = base_url.trim_start_matches("http://").to_string();
    let head_of_body = br#"{"query":"subscription { "#;
    let declared = head_of_body.len() + 1024;
    let mut head = format!(
        "POST /graphql HTTP/1.1\r\nHost: {addr}\r\nAccept: {EVENT_STREAM}\r\n\
         Content-Type: application/json\r\nContent-Length: {declared}\r\n"
    );
    if let Some(passport) = passport {
        head.push_str(&format!("x-passport: {passport}\r\n"));
    }
    head.push_str("\r\n");
    let mut stream = TcpStream::connect(&addr).await.expect("connect to the pod");
    stream
        .write_all(head.as_bytes())
        .await
        .expect("send the head");
    stream
        .write_all(head_of_body)
        .await
        .expect("send the start of the body");
    let mut answer = vec![0_u8; 1024];
    match tokio::time::timeout(wait, stream.read(&mut answer)).await {
        Err(_) => None,
        Ok(read) => {
            let read = read.expect("read the answer");
            Some(String::from_utf8_lossy(&answer[..read]).into_owned())
        }
    }
}
