use std::time::Duration;

use async_graphql::{Data, ObjectType, Schema, SubscriptionType};
use async_graphql_axum::{GraphQLProtocol, GraphQLWebSocket};
use axum::extract::ws::{CloseFrame, Message, WebSocket, close_code};
use futures_channel::mpsc;
use futures_util::{SinkExt, StreamExt};

pub const SESSION_MAX_AGE_CLOSE_CODE: u16 = close_code::AWAY;
pub const SESSION_MAX_AGE_CLOSE_REASON: &str =
    "session_max_age reached; reconnect with a fresh passport";

pub const SHUTDOWN_CLOSE_CODE: u16 = close_code::AWAY;
pub const SHUTDOWN_CLOSE_REASON: &str =
    "the service is shutting down; reconnect with a fresh passport";

const OUTBOUND_BUFFER: usize = 32;

enum Ended {
    Expired,
    ShuttingDown,
    Closed,
}

pub async fn serve_bounded<Q, M, S>(
    socket: WebSocket,
    schema: Schema<Q, M, S>,
    protocol: GraphQLProtocol,
    data: Data,
    max_age: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) where
    Q: ObjectType + 'static,
    M: ObjectType + 'static,
    S: SubscriptionType + 'static,
{
    let (mut sink, stream) = socket.split();
    if *shutdown.borrow_and_update() {
        send_close(&mut sink, SHUTDOWN_CLOSE_CODE, SHUTDOWN_CLOSE_REASON).await;
        return;
    }
    let (outbound, mut delivered) = mpsc::channel::<Message>(OUTBOUND_BUFFER);
    let serve = GraphQLWebSocket::new_with_pair(outbound, stream, schema, protocol)
        .with_data(data)
        .serve();
    futures_util::pin_mut!(serve);
    let deadline = tokio::time::sleep(max_age);
    futures_util::pin_mut!(deadline);

    let mut serve_done = false;
    let ended = loop {
        tokio::select! {
            biased;
            () = &mut deadline => break Ended::Expired,
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break Ended::ShuttingDown;
                }
            }
            () = &mut serve, if !serve_done => serve_done = true,
            message = delivered.next() => match message {
                Some(message) => {
                    if sink.send(message).await.is_err() {
                        break Ended::Closed;
                    }
                }
                None => break Ended::Closed,
            },
        }
    };

    match ended {
        Ended::Expired => {
            send_close(&mut sink, SESSION_MAX_AGE_CLOSE_CODE, SESSION_MAX_AGE_CLOSE_REASON).await;
        }
        Ended::ShuttingDown => {
            send_close(&mut sink, SHUTDOWN_CLOSE_CODE, SHUTDOWN_CLOSE_REASON).await;
        }
        Ended::Closed => {}
    }
    let _ = sink.close().await;
}

async fn send_close<Si>(sink: &mut Si, code: u16, reason: &'static str)
where
    Si: SinkExt<Message> + Unpin,
{
    let _ = sink
        .send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        })))
        .await;
}
