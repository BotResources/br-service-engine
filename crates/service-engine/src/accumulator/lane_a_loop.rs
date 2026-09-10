use std::sync::Arc;
use std::time::{Duration, Instant};

use async_nats::jetstream::consumer::Consumer;
use async_nats::jetstream::consumer::pull::Config as PullConfig;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::accumulator::ingress::StreamingIngress;
use crate::accumulator::runtime::AccumulatorRuntime;
use crate::accumulator::seal;
use crate::chain::describe;
use crate::error::EngineError;
use crate::inbound::{HealthTracker, InboundHealth, ServeExit, SupervisorConfig};
use crate::nats::{Nats, streaming_stream, subject_token};

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
    health: InboundHealth,
    stop_ingress: Arc<Notify>,
    stop_purge: Arc<Notify>,
) -> Result<(JoinHandle<()>, JoinHandle<()>), EngineError> {
    let (_ingress, _consumer) = establish(
        nats,
        &service,
        seal_retention,
        accumulators.clone(),
        ack_wait,
        max_ack_pending,
    )
    .await?;
    let ingress_task = tokio::spawn(run_lane_a_supervised(
        nats.clone(),
        service.clone(),
        seal_retention,
        accumulators.clone(),
        ack_wait,
        max_ack_pending,
        health,
        stop_ingress,
    ));
    let purge_task = tokio::spawn(run_purge(
        nats.clone(),
        accumulators,
        service,
        beat,
        stop_purge,
    ));
    Ok((ingress_task, purge_task))
}

#[allow(clippy::too_many_arguments)]
async fn run_lane_a_supervised(
    nats: Nats,
    service: String,
    seal_retention: Duration,
    accumulators: Arc<AccumulatorRuntime>,
    ack_wait: Duration,
    max_ack_pending: i64,
    health: InboundHealth,
    stop: Arc<Notify>,
) {
    let cfg = SupervisorConfig::default();
    let mut tracker = HealthTracker::new(
        format!("streaming:{service}"),
        health,
        cfg.failure_threshold,
    );
    loop {
        let established = establish(
            &nats,
            &service,
            seal_retention,
            accumulators.clone(),
            ack_wait,
            max_ack_pending,
        )
        .await;
        let (ingress, consumer) = match established {
            Ok(pair) => pair,
            Err(error) => {
                let delay = tracker.failed(Duration::ZERO, cfg.healthy_uptime, &describe(&error));
                if sleep_or_notified(delay, &stop).await {
                    return;
                }
                continue;
            }
        };
        tracker.connected();
        let started = Instant::now();
        let exit = ingress
            .serve_with_promotion(consumer, &stop, &mut tracker, cfg.healthy_uptime)
            .await;
        match exit {
            Ok(ServeExit::Cancelled) => return,
            Ok(ServeExit::Ended) => {
                let delay = tracker.failed(
                    started.elapsed(),
                    cfg.healthy_uptime,
                    "the lane-A ingress stream ended",
                );
                if sleep_or_notified(delay, &stop).await {
                    return;
                }
            }
            Err(error) => {
                let delay =
                    tracker.failed(started.elapsed(), cfg.healthy_uptime, &describe(&error));
                if sleep_or_notified(delay, &stop).await {
                    return;
                }
            }
        }
    }
}

async fn sleep_or_notified(delay: Duration, stop: &Arc<Notify>) -> bool {
    tokio::select! {
        biased;
        () = stop.notified() => true,
        () = tokio::time::sleep(delay) => false,
    }
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
