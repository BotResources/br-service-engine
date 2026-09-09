use std::time::Duration;

use async_graphql::{Data, ObjectType, Schema, SubscriptionType};
use async_graphql_axum::{GraphQLProtocol, GraphQLWebSocket};
use axum::extract::ws::{CloseFrame, Message, WebSocket, close_code};
use futures_channel::mpsc;
use futures_util::{SinkExt, StreamExt};

pub const SESSION_MAX_AGE_CLOSE_CODE: u16 = close_code::AWAY;
pub const SESSION_MAX_AGE_CLOSE_REASON: &str =
    "session_max_age reached; reconnect with a fresh passport";

const OUTBOUND_BUFFER: usize = 32;

pub async fn serve_bounded<Q, M, S>(
    socket: WebSocket,
    schema: Schema<Q, M, S>,
    protocol: GraphQLProtocol,
    data: Data,
    max_age: Duration,
) where
    Q: ObjectType + 'static,
    M: ObjectType + 'static,
    S: SubscriptionType + 'static,
{
    let (mut sink, stream) = socket.split();
    let (outbound, mut delivered) = mpsc::channel::<Message>(OUTBOUND_BUFFER);
    let serve = GraphQLWebSocket::new_with_pair(outbound, stream, schema, protocol)
        .with_data(data)
        .serve();
    futures_util::pin_mut!(serve);
    let deadline = tokio::time::sleep(max_age);
    futures_util::pin_mut!(deadline);

    let mut serve_done = false;
    let expired = loop {
        tokio::select! {
            biased;
            () = &mut deadline => break true,
            () = &mut serve, if !serve_done => serve_done = true,
            message = delivered.next() => match message {
                Some(message) => {
                    if sink.send(message).await.is_err() {
                        break false;
                    }
                }
                None => break false,
            },
        }
    };

    if expired {
        let _ = sink
            .send(Message::Close(Some(CloseFrame {
                code: SESSION_MAX_AGE_CLOSE_CODE,
                reason: SESSION_MAX_AGE_CLOSE_REASON.into(),
            })))
            .await;
    }
    let _ = sink.close().await;
}
