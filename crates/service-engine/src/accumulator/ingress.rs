use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::AckKind;
use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::consumer::{AckPolicy, Consumer, DeliverPolicy, ReplayPolicy};
use futures_util::{FutureExt, StreamExt};

use crate::accumulator::ChunkSeq;
use crate::accumulator::runtime::AccumulatorRuntime;
use crate::chain::describe;
use crate::error::{AccumulatorError, EngineError};
use crate::inbound::{HealthTracker, ServeExit};
use crate::name::AccumulatorName;
use crate::nats::{Nats, StreamFrame, streaming_filter, streaming_stream};
use crate::stop::Stop;

const INACTIVE_THRESHOLD: Duration = Duration::from_secs(300);

pub struct StreamingIngress {
    nats: Nats,
    service: String,
    accumulators: Arc<AccumulatorRuntime>,
    ack_wait: Duration,
    max_ack_pending: i64,
    nak_backoff: Duration,
}

impl StreamingIngress {
    pub fn new(
        nats: Nats,
        service: String,
        accumulators: Arc<AccumulatorRuntime>,
        ack_wait: Duration,
        max_ack_pending: i64,
    ) -> Self {
        Self {
            nats,
            service,
            accumulators,
            ack_wait,
            max_ack_pending,
            nak_backoff: Duration::from_millis(50),
        }
    }

    pub async fn open(&self) -> Result<Consumer<PullConfig>, EngineError> {
        let stream = self
            .nats
            .bind_stream(&streaming_stream(&self.service))
            .await?;
        stream
            .create_consumer(PullConfig {
                durable_name: None,
                name: None,
                ack_policy: AckPolicy::Explicit,
                deliver_policy: DeliverPolicy::All,
                replay_policy: ReplayPolicy::Instant,
                filter_subject: streaming_filter(&self.service),
                ack_wait: self.ack_wait,
                max_ack_pending: self.max_ack_pending,
                inactive_threshold: INACTIVE_THRESHOLD,
                ..Default::default()
            })
            .await
            .map_err(|error| {
                EngineError::Config(format!(
                    "the lane-A ingress consumer could not be created on {}: {}",
                    streaming_stream(&self.service),
                    describe(&error)
                ))
            })
    }

    async fn serve(
        &self,
        consumer: Consumer<PullConfig>,
        stop: &Arc<Stop>,
    ) -> Result<ServeExit, EngineError> {
        let mut messages = consumer.messages().await.map_err(|error| {
            EngineError::Config(format!(
                "the lane-A ingress could not open its message stream: {}",
                describe(&error)
            ))
        })?;
        let stopping = stop.stopped();
        tokio::pin!(stopping);
        loop {
            let message = tokio::select! {
                biased;
                () = &mut stopping => return Ok(ServeExit::Cancelled),
                next = messages.next() => next,
            };
            match message {
                Some(Ok(message)) => self.fold(&message).await,
                Some(Err(error)) => {
                    return Err(EngineError::Config(format!(
                        "the lane-A ingress lost its message stream: {}",
                        describe(&error)
                    )));
                }
                None => return Ok(ServeExit::Ended),
            }
        }
    }

    pub(crate) async fn serve_with_promotion(
        &self,
        consumer: Consumer<PullConfig>,
        stop: &Arc<Stop>,
        tracker: &mut HealthTracker,
        uptime: Duration,
    ) -> Result<ServeExit, EngineError> {
        let served = AssertUnwindSafe(self.serve(consumer, stop)).catch_unwind();
        tokio::pin!(served);
        let promote = tokio::time::sleep(uptime);
        tokio::pin!(promote);
        let mut promoted = false;
        loop {
            tokio::select! {
                biased;
                exit = &mut served => return exit.unwrap_or_else(|_| {
                    Err(EngineError::Config(
                        "the lane-A ingress panicked while folding a chunk; it restarts under \
                         supervision and, past the threshold, lowers readiness"
                            .to_string(),
                    ))
                }),
                () = &mut promote, if !promoted => {
                    tracker.promote();
                    promoted = true;
                }
            }
        }
    }

    async fn fold(&self, message: &async_nats::jetstream::Message) {
        let frame: StreamFrame = match serde_json::from_slice(&message.payload) {
            Ok(frame) => frame,
            Err(error) => {
                tracing::warn!(error = %describe(&error), subject = %message.subject, "a lane-A chunk is undecodable");
                return self.terminate(message).await;
            }
        };
        let name = match AccumulatorName::new(frame.accumulator.clone()) {
            Ok(name) => name,
            Err(error) => {
                tracing::warn!(reason = %describe(&error), "a lane-A chunk names an invalid accumulator");
                return self.terminate(message).await;
            }
        };
        let seq = match ChunkSeq::new(frame.seq) {
            Ok(seq) => seq,
            Err(error) => {
                tracing::warn!(reason = %describe(&error), "a lane-A chunk carries an out-of-range sequence");
                return self.terminate(message).await;
            }
        };
        match self
            .accumulators
            .push_frame(&name, &frame.key, seq, frame.chunk)
        {
            Ok(_receipt) => self.confirm(message).await,
            Err(EngineError::Accumulator(AccumulatorError::ChunkBufferFull { .. })) => {
                self.retry(message).await
            }
            Err(error) => {
                tracing::warn!(reason = %describe(&error), "a lane-A chunk cannot be routed to an accumulator");
                self.terminate(message).await;
            }
        }
    }

    async fn confirm(&self, message: &async_nats::jetstream::Message) {
        if let Err(error) = message.ack().await {
            tracing::warn!(error = %describe(&*error), "acking a folded lane-A chunk failed; the stream still holds it for replay");
        }
    }

    async fn retry(&self, message: &async_nats::jetstream::Message) {
        if let Err(error) = message.ack_with(AckKind::Nak(Some(self.nak_backoff))).await {
            tracing::warn!(error = %describe(&*error), "naking a lane-A chunk under back-pressure failed");
        }
    }

    async fn terminate(&self, message: &async_nats::jetstream::Message) {
        if let Err(error) = message.ack_with(AckKind::Term).await {
            tracing::warn!(error = %describe(&*error), "terminating a malformed lane-A chunk failed");
        }
    }
}
