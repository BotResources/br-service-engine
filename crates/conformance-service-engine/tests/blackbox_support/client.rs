use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub async fn post_json(
    base_url: &str,
    passport: Option<&str>,
    query: &str,
    variables: serde_json::Value,
) -> (u16, serde_json::Value) {
    let client = reqwest::Client::new();
    let mut request = client
        .post(format!("{base_url}/graphql"))
        .json(&serde_json::json!({ "query": query, "variables": variables }));
    if let Some(passport) = passport {
        request = request.header("x-passport", passport);
    }
    let response = request
        .send()
        .await
        .expect("the graphql POST reaches the running binary");
    let status = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    let json = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    (status, json)
}

pub struct GraphqlWs {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl GraphqlWs {
    pub async fn connect(base_url: &str, passport: &str) -> Self {
        let ws_url = format!("{}/graphql/ws", base_url.replacen("http://", "ws://", 1));
        let mut request = ws_url
            .into_client_request()
            .expect("the ws url is a valid client request");
        request.headers_mut().insert(
            "sec-websocket-protocol",
            HeaderValue::from_static("graphql-transport-ws"),
        );
        request.headers_mut().insert(
            "x-passport",
            HeaderValue::from_str(passport).expect("the passport is a valid header value"),
        );
        let (stream, _response) = tokio_tungstenite::connect_async(request)
            .await
            .expect("the subscription transport upgrades to a websocket");
        let mut ws = Self { stream };
        ws.send(serde_json::json!({ "type": "connection_init" }))
            .await;
        ws.expect_type("connection_ack").await;
        ws
    }

    pub async fn subscribe(&mut self, id: &str, query: &str) {
        self.send(serde_json::json!({
            "id": id,
            "type": "subscribe",
            "payload": { "query": query },
        }))
        .await;
    }

    pub async fn next_data(&mut self, within: Duration) -> Option<serde_json::Value> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let message = match tokio::time::timeout(remaining, self.stream.next()).await {
                Err(_) => return None,
                Ok(None) => return None,
                Ok(Some(message)) => message.expect("the websocket yields a frame"),
            };
            let Ok(text) = message.to_text() else {
                continue;
            };
            if text.is_empty() {
                continue;
            }
            let value: serde_json::Value = match serde_json::from_str(text) {
                Ok(value) => value,
                Err(_) => continue,
            };
            match value.get("type").and_then(|t| t.as_str()) {
                Some("next") => {
                    return value
                        .get("payload")
                        .and_then(|payload| payload.get("data"))
                        .cloned();
                }
                Some("error") => panic!("the subscription reported an error: {value}"),
                _ => continue,
            }
        }
    }

    async fn send(&mut self, value: serde_json::Value) {
        self.stream
            .send(Message::text(value.to_string()))
            .await
            .expect("a websocket frame is sent");
    }

    async fn expect_type(&mut self, expected: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(!remaining.is_zero(), "never received a {expected} frame");
            let message = tokio::time::timeout(remaining, self.stream.next())
                .await
                .expect("the ws handshake does not time out")
                .expect("the ws stream stays open during the handshake")
                .expect("the ws yields a frame");
            let Ok(text) = message.to_text() else {
                continue;
            };
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(text)
                && value.get("type").and_then(|t| t.as_str()) == Some(expected)
            {
                return;
            }
        }
    }
}
