use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::AckKind;
use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::consumer::{AckPolicy, Consumer, DeliverPolicy, ReplayPolicy};
use futures_util::StreamExt;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::accumulator::runtime::AccumulatorRuntime;
use crate::accumulator::{ChunkSeq, seal};
use crate::chain::describe;
use crate::error::EngineError;
use crate::name::AccumulatorName;
use crate::nats::{Nats, StreamFrame, streaming_filter, streaming_stream, subject_token};

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
                    "the lane-A ingress consumer could not be created on {}: {error}",
                    streaming_stream(&self.service)
                ))
            })
    }

    pub async fn serve(
        self,
        consumer: Consumer<PullConfig>,
        shutdown: Arc<Notify>,
    ) -> Result<(), EngineError> {
        let mut messages = consumer.messages().await.map_err(|error| {
            EngineError::Config(format!(
                "the lane-A ingress could not open its message stream: {error}"
            ))
        })?;
        let stopping = shutdown.notified();
        tokio::pin!(stopping);
        loop {
            let message = tokio::select! {
                biased;
                () = &mut stopping => return Ok(()),
                next = messages.next() => next,
            };
            match message {
                Some(Ok(message)) => self.fold(&message).await,
                Some(Err(error)) => tracing::warn!(
                    reason = %error,
                    "the lane-A ingress stream yielded an error; the consumer continues"
                ),
                None => return Ok(()),
            }
        }
    }

    async fn fold(&self, message: &async_nats::jetstream::Message) {
        let frame: StreamFrame = match serde_json::from_slice(&message.payload) {
            Ok(frame) => frame,
            Err(error) => {
                tracing::warn!(%error, subject = %message.subject, "a lane-A chunk is undecodable");
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
            Err(EngineError::ChunkBufferFull { .. }) => self.retry(message).await,
            Err(error) => {
                tracing::warn!(reason = %describe(&error), "a lane-A chunk cannot be routed to an accumulator");
                self.terminate(message).await;
            }
        }
    }

    async fn confirm(&self, message: &async_nats::jetstream::Message) {
        if let Err(error) = message.ack().await {
            tracing::warn!(%error, "acking a folded lane-A chunk failed; the stream still holds it for replay");
        }
    }

    async fn retry(&self, message: &async_nats::jetstream::Message) {
        if let Err(error) = message.ack_with(AckKind::Nak(Some(self.nak_backoff))).await {
            tracing::warn!(%error, "naking a lane-A chunk under back-pressure failed");
        }
    }

    async fn terminate(&self, message: &async_nats::jetstream::Message) {
        if let Err(error) = message.ack_with(AckKind::Term).await {
            tracing::warn!(%error, "terminating a malformed lane-A chunk failed");
        }
    }
}

pub(crate) async fn establish(
    nats: &Nats,
    service: &str,
    seal_retention: Duration,
    accumulators: Arc<AccumulatorRuntime>,
    ack_wait: Duration,
    max_ack_pending: i64,
) -> Result<(StreamingIngress, Consumer<PullConfig>), EngineError> {
    let stream = streaming_stream(service);
    let max_age = nats.stream_max_age(&stream).await?;
    if max_age.is_zero() || seal_retention < max_age {
        return Err(EngineError::SealRetentionTooShort {
            stream,
            seal_retention,
            max_age,
        });
    }
    let ingress = StreamingIngress::new(
        nats.clone(),
        service.to_string(),
        accumulators,
        ack_wait,
        max_ack_pending,
    );
    let consumer = ingress.open().await?;
    Ok((ingress, consumer))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn spawn_lane_a(
    nats: &Nats,
    service: String,
    seal_retention: Duration,
    accumulators: Arc<AccumulatorRuntime>,
    beat: Duration,
    ack_wait: Duration,
    max_ack_pending: i64,
    stop_ingress: Arc<Notify>,
    stop_purge: Arc<Notify>,
) -> Result<(JoinHandle<Result<(), EngineError>>, JoinHandle<()>), EngineError> {
    let (ingress, consumer) = establish(
        nats,
        &service,
        seal_retention,
        accumulators.clone(),
        ack_wait,
        max_ack_pending,
    )
    .await?;
    let ingress_task = tokio::spawn(ingress.serve(consumer, stop_ingress));
    let purge_task = tokio::spawn(run_purge(
        nats.clone(),
        accumulators,
        service,
        beat,
        stop_purge,
    ));
    Ok((ingress_task, purge_task))
}

pub(crate) async fn run_purge(
    nats: Nats,
    accumulators: Arc<AccumulatorRuntime>,
    service: String,
    interval: Duration,
    shutdown: Arc<Notify>,
) {
    let stopping = shutdown.notified();
    tokio::pin!(stopping);
    loop {
        tokio::select! {
            biased;
            () = &mut stopping => return,
            () = tokio::time::sleep(interval) => {
                if let Err(error) = purge_sealed(&nats, &accumulators, &service).await {
                    tracing::warn!(
                        reason = %describe(&error),
                        "the beat could not purge a sealed lane-A subject; it retries next tick"
                    );
                }
            }
        }
    }
}

pub async fn purge_sealed(
    nats: &Nats,
    accumulators: &AccumulatorRuntime,
    service: &str,
) -> Result<u64, EngineError> {
    let stream = streaming_stream(service);
    let pool = accumulators.reader().pool();
    let pending = seal::unpurged(pool).await?;
    let mut purged = 0;
    for (accumulator, key) in pending {
        let token = subject_token(&key)?;
        nats.purge_chunk_subject(&stream, &crate::nats::chunk_subject(service, &token))
            .await?;
        seal::mark_purged(pool, &accumulator, &key).await?;
        purged += 1;
    }
    Ok(purged)
}
