use async_nats::jetstream::AckKind;
use bytes::Bytes;
use uuid::Uuid;

use crate::inbound::budget::{Route, route};
use crate::inbound::consumer::InboundConsumer;
use crate::inbound::deadletter::DeadLetterSource;
use crate::inbound::dispatch::{DispatchError, DispatchOutcome};
use crate::inbound::message::{Incoming, Source};

impl InboundConsumer {
    pub(crate) async fn handle(&self, message: &async_nats::jetstream::Message) {
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
            metadata: crate::inbound::MessageMetadata::default(),
            body: payload.clone(),
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
