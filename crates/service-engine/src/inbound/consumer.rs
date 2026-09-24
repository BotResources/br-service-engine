use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use std::time::{Duration, Instant};

use async_nats::jetstream::AckKind;
use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::consumer::{AckPolicy, Consumer, DeliverPolicy, ReplayPolicy};
use futures_util::{FutureExt, StreamExt};
use tokio::sync::watch;

use crate::chain::describe;
use crate::error::EngineError;
use crate::inbound::deadletter::DeadLetters;
use crate::inbound::dispatch::Dispatch;
use crate::inbound::subscription::{InboundConfig, Subscription};
use crate::inbound::supervisor::{
    HealthTracker, InboundHealth, ServeExit, SupervisorConfig, sleep_or_cancel,
};
use crate::nats::Nats;

pub struct InboundConsumer {
    nats: Nats,
    pub(crate) subscription: Subscription,
    pub(crate) dispatch: Arc<dyn Dispatch>,
    pub(crate) dead_letters: DeadLetters,
    pub(crate) config: InboundConfig,
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
                    "the inbound durable {} could not be created on {}: {}",
                    self.subscription.reaction,
                    coordinates.stream(),
                    describe(&error)
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
        let mut tracker = HealthTracker::new(
            self.subscription.reaction.clone(),
            health,
            cfg.failure_threshold,
        );
        loop {
            if *cancel.borrow() {
                break;
            }
            let consumer = match self.open().await {
                Ok(consumer) => consumer,
                Err(error) => {
                    let delay =
                        tracker.failed(Duration::ZERO, cfg.healthy_uptime, &describe(&error));
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
        let served = AssertUnwindSafe(self.serve(consumer, cancel)).catch_unwind();
        tokio::pin!(served);
        let promote = tokio::time::sleep(uptime);
        tokio::pin!(promote);
        let mut promoted = false;
        loop {
            tokio::select! {
                biased;
                exit = &mut served => return exit.unwrap_or_else(|_| {
                    Err(EngineError::Config(format!(
                        "the inbound durable {} panicked while dispatching; it restarts under \
                         supervision and, past the threshold, lowers readiness",
                        self.subscription.reaction
                    )))
                }),
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
                "the inbound durable {} could not open its message stream: {}",
                self.subscription.reaction,
                describe(&error)
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
                        "the inbound durable {} lost its message stream: {}",
                        self.subscription.reaction,
                        describe(&error)
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
                            error = %describe(&*error),
                            "naking a pulled-but-unprocessed frame on shutdown failed; it \
                             redelivers to a live pod"
                        );
                    }
                }
                _ => break,
            }
        }
    }
}
