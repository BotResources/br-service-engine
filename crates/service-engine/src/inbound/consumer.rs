use std::sync::Arc;

use std::time::{Duration, Instant};

use async_nats::jetstream::AckKind;
use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::consumer::{AckPolicy, Consumer, DeliverPolicy, ReplayPolicy};
use bytes::Bytes;
use futures_util::{FutureExt, StreamExt};
use tokio::sync::watch;
use uuid::Uuid;

use crate::chain::describe;
use crate::error::EngineError;
use crate::inbound::budget::{Route, route};
use crate::inbound::deadletter::{DeadLetterSource, DeadLetters};
use crate::inbound::dispatch::{Dispatch, DispatchError, DispatchOutcome};
use crate::inbound::message::{Incoming, Source};
use crate::inbound::subscription::{InboundConfig, Subscription};
use crate::inbound::supervisor::{
    HealthTracker, InboundHealth, ServeExit, SupervisorConfig, sleep_or_cancel,
};
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

    pub(crate) async fn run_supervised(
        self,
        mut cancel: watch::Receiver<bool>,
        health: InboundHealth,
        cfg: SupervisorConfig,
    ) {
        let mut tracker =
            HealthTracker::new(self.subscription.reaction.clone(), health, cfg.failure_threshold);
        loop {
            if *cancel.borrow() {
                break;
            }
            let consumer = match self.open().await {
                Ok(consumer) => consumer,
                Err(error) => {
                    let delay = tracker.failed(Duration::ZERO, cfg.healthy_uptime, &describe(&error));
                    if sleep_or_cancel(delay, &mut cancel).await {
                        break;
                    }
                    continue;
                }
            };
            tracker.connected();
            let started = Instant::now();
            let exit = self
                .serve_with_promotion(consumer, &mut cancel, &mut tracker, cfg.healthy_uptime)
                .await;
            match exit {
                Ok(ServeExit::Cancelled) => break,
                Ok(ServeExit::Ended) => {
                    let delay = tracker.failed(
                        started.elapsed(),
                        cfg.healthy_uptime,
                        "the inbound message stream ended",
                    );
                    if sleep_or_cancel(delay, &mut cancel).await {
                        break;
                    }
                }
                Err(error) => {
                    let delay =
                        tracker.failed(started.elapsed(), cfg.healthy_uptime, &describe(&error));
                    if sleep_or_cancel(delay, &mut cancel).await {
                        break;
                    }
                }
            }
        }
    }

    async fn serve_with_promotion(
        &self,
        consumer: Consumer<PullConfig>,
        cancel: &mut watch::Receiver<bool>,
        tracker: &mut HealthTracker,
        uptime: Duration,
    ) -> Result<ServeExit, EngineError> {
        let served = self.serve(consumer, cancel);
        tokio::pin!(served);
        let promote = tokio::time::sleep(uptime);
        tokio::pin!(promote);
        let mut promoted = false;
        loop {
            tokio::select! {
                biased;
                exit = &mut served => return exit,
                () = &mut promote, if !promoted => {
                    tracker.promote();
                    promoted = true;
                }
            }
        }
    }

    async fn serve(
        &self,
        consumer: Consumer<PullConfig>,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<ServeExit, EngineError> {
        let mut messages = consumer.messages().await.map_err(|error| {
            EngineError::Config(format!(
                "the inbound durable {} could not open its message stream: {error}",
                self.subscription.reaction
            ))
        })?;
        loop {
            let message = tokio::select! {
                biased;
                _ = cancel.changed() => {
                    self.drain_naks(&mut messages).await;
                    return Ok(ServeExit::Cancelled);
                }
                next = messages.next() => next,
            };
            match message {
                Some(Ok(message)) => self.handle(&message).await,
                Some(Err(error)) => {
                    return Err(EngineError::Config(format!(
                        "the inbound durable {} lost its message stream: {error}",
                        self.subscription.reaction
                    )));
                }
                None => return Ok(ServeExit::Ended),
            }
        }
    }

    async fn drain_naks(&self, messages: &mut async_nats::jetstream::consumer::pull::Stream) {
        for _ in 0..self.config.max_ack_pending.max(0) {
            match messages.next().now_or_never() {
                Some(Some(Ok(message))) => {
                    if let Err(error) = message.ack_with(AckKind::Nak(None)).await {
                        tracing::warn!(
                            reaction = %self.subscription.reaction,
                            %error,
                            "naking a pulled-but-unprocessed frame on shutdown failed; it \
                             redelivers to a live pod"
                        );
                    }
                }
                _ => break,
            }
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
