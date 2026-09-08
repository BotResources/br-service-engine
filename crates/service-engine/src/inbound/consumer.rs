use std::sync::Arc;

use async_nats::jetstream::AckKind;
use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::consumer::{AckPolicy, Consumer, DeliverPolicy, ReplayPolicy};
use bytes::Bytes;
use futures_util::StreamExt;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::error::EngineError;
use crate::inbound::budget::{Route, route};
use crate::inbound::deadletter::{DeadLetterSource, DeadLetters};
use crate::inbound::dispatch::{Dispatch, DispatchError, DispatchOutcome};
use crate::inbound::message::{Incoming, Source};
use crate::inbound::subscription::{InboundConfig, Subscription};
use crate::nats::Nats;

pub struct InboundConsumer {
    nats: Nats,
    subscription: Subscription,
    dispatch: Arc<dyn Dispatch>,
    dead_letters: DeadLetters,
    config: InboundConfig,
}

impl InboundConsumer {
    pub fn new(
        nats: Nats,
        subscription: Subscription,
        dispatch: Arc<dyn Dispatch>,
        dead_letters: DeadLetters,
        config: InboundConfig,
    ) -> Self {
        Self {
            nats,
            subscription,
            dispatch,
            dead_letters,
            config,
        }
    }

    pub async fn open(&self) -> Result<Consumer<PullConfig>, EngineError> {
        let coordinates = &self.subscription.coordinates;
        let stream = self.nats.bind_stream(coordinates.stream()).await?;
        let consumer = stream
            .create_consumer(PullConfig {
                durable_name: Some(self.subscription.reaction.clone()),
                name: Some(self.subscription.reaction.clone()),
                ack_policy: AckPolicy::Explicit,
                deliver_policy: DeliverPolicy::All,
                replay_policy: ReplayPolicy::Instant,
                filter_subject: coordinates.subject(),
                ack_wait: self.config.ack_wait,
                max_deliver: -1,
                max_ack_pending: self.config.max_ack_pending,
                ..Default::default()
            })
            .await
            .map_err(|error| {
                EngineError::Config(format!(
                    "the inbound durable {} could not be created on {}: {error}",
                    self.subscription.reaction,
                    coordinates.stream()
                ))
            })?;
        Ok(consumer)
    }

    pub async fn serve(
        self,
        consumer: Consumer<PullConfig>,
        cancel: Arc<Notify>,
    ) -> Result<(), EngineError> {
        let mut messages = consumer.messages().await.map_err(|error| {
            EngineError::Config(format!(
                "the inbound durable {} could not open its message stream: {error}",
                self.subscription.reaction
            ))
        })?;
        loop {
            let message = tokio::select! {
                biased;
                () = cancel.notified() => return Ok(()),
                next = messages.next() => next,
            };
            let message = match message {
                Some(Ok(message)) => message,
                Some(Err(error)) => {
                    tracing::warn!(
                        reaction = %self.subscription.reaction,
                        %error,
                        "the inbound message stream yielded an error; the consumer continues"
                    );
                    continue;
                }
                None => return Ok(()),
            };
            self.handle(&message).await;
        }
    }

    async fn handle(&self, message: &async_nats::jetstream::Message) {
        let delivered = message
            .info()
            .map(|info| info.delivered.max(1) as u32)
            .unwrap_or(1);
        let subject = message.subject.to_string();
        let source = self.subscription.coordinates.source();
        let incoming = match Incoming::identify(
            &self.subscription.reaction,
            source,
            subject.clone(),
            message.headers.as_ref(),
            message.payload.clone(),
            delivered,
        ) {
            Ok(incoming) => incoming,
            Err(error) => {
                self.dead_letter(
                    &self.unidentified(source, subject, message.payload.clone(), delivered),
                    &error.to_string(),
                )
                .await;
                self.terminate(message).await;
                return;
            }
        };

        match self.dispatch.dispatch(&incoming).await {
            DispatchOutcome::Applied(_) => self.confirm(message).await,
            DispatchOutcome::Failed(error) => self.route(message, &incoming, error).await,
        }
    }

    fn unidentified(
        &self,
        source: Source,
        subject: String,
        payload: Bytes,
        delivered: u32,
    ) -> Incoming {
        Incoming {
            reaction: self.subscription.reaction.clone(),
            source,
            subject,
            message_id: Uuid::now_v7(),
            sequence: None,
            payload,
            delivered,
        }
    }

    async fn route(
        &self,
        message: &async_nats::jetstream::Message,
        incoming: &Incoming,
        error: DispatchError,
    ) {
        let route = route(
            error.disposition,
            incoming.delivered,
            self.subscription.budgets,
            self.config.backoff_base,
            self.config.backoff_max,
        );
        match route {
            Route::Nak(delay) => {
                if let Err(ack) = message.ack_with(AckKind::Nak(Some(delay))).await {
                    tracing::warn!(reaction = %incoming.reaction, %ack, "nak failed");
                }
            }
            Route::DeadLetter => {
                self.dead_letter(incoming, &error.detail).await;
                self.terminate(message).await;
            }
        }
    }

    async fn dead_letter(&self, incoming: &Incoming, detail: &str) {
        if let Err(error) = self
            .dead_letters
            .record(DeadLetterSource::Reaction, incoming, detail)
            .await
        {
            tracing::error!(
                reaction = %incoming.reaction,
                %error,
                "a poison message could not be written to the dead-letter table"
            );
        }
    }

    async fn confirm(&self, message: &async_nats::jetstream::Message) {
        if let Err(error) = message.double_ack().await {
            tracing::warn!(
                reaction = %self.subscription.reaction,
                %error,
                "the ack after a durable effect failed; a redelivery will find the claim and ack again"
            );
        }
    }

    async fn terminate(&self, message: &async_nats::jetstream::Message) {
        if let Err(error) = message.ack_with(AckKind::Term).await {
            tracing::warn!(
                reaction = %self.subscription.reaction,
                %error,
                "terminating a dead-lettered delivery failed; it will redeliver and dead-letter again"
            );
        }
    }
}
