use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::event_stream::{Block, EventStreamParser};

pub const EVENT_STREAM: &str = "text/event-stream";

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
    parser: EventStreamParser,
}

impl SseFrames {
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
            parser: EventStreamParser::default(),
            body: Box::pin(response.bytes_stream()),
        }
    }

    pub async fn next_frame(&mut self, within: Duration) -> Option<Frame> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            if let Some(block) = self.parser.next_block() {
                return Some(parse(&block));
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, self.body.next()).await {
                Err(_) => panic!(
                    "no whole frame within {within:?}; pending: {}",
                    self.parser.unterminated()
                ),
                Ok(None) => {
                    assert!(
                        !self.parser.has_unterminated_input(),
                        "the body ended inside a frame: {}",
                        self.parser.unterminated()
                    );
                    return None;
                }
                Ok(Some(chunk)) => {
                    self.parser
                        .push(&chunk.expect("the event stream is readable"));
                }
            }
        }
    }

    pub async fn next_event(&mut self, within: Duration) -> Option<Frame> {
        loop {
            match self.next_frame(within).await {
                Some(Frame::Comment) => continue,
                other => return other,
            }
        }
    }

    pub async fn expect_event_on(&mut self, field: &str, within: Duration) -> serde_json::Value {
        assert_eq!(self.status, 200, "the subscription request is answered 200");
        match self.next_event(within).await {
            Some(Frame::Next(payload)) => {
                assert!(
                    payload["errors"].is_null(),
                    "the subscription returned errors: {payload}"
                );
                let delta = payload["data"][field].clone();
                assert!(
                    !delta.is_null(),
                    "the event carries no field {field:?}: {payload}"
                );
                delta
            }
            other => panic!("expected a next frame carrying {field:?}, got {other:?}"),
        }
    }
}

fn parse(block: &Block) -> Frame {
    if block.is_comment_only() {
        return Frame::Comment;
    }
    match (block.event.as_deref(), block.data.as_deref()) {
        (Some("next"), Some(data)) => {
            Frame::Next(serde_json::from_str(data).expect("a next frame carries JSON"))
        }
        (Some("complete"), Some("")) => Frame::Complete,
        _ => panic!("not a graphql-sse frame: {block:?}"),
    }
}

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
