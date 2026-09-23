use std::sync::Arc;

use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use serde_json::Value;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use crate::error::{EngineError, TransportError};
use crate::impact::{Impact, TransportEvent};
use crate::nats::{KvBucket, KvEvent, Nats};
use crate::presence::lane::PresenceLane;
use crate::principal::Principal;
use crate::stop::Stop;

pub(crate) type PresenceImpacts = UnboundedSender<Result<TransportEvent, TransportError>>;

pub(crate) fn presence_channel() -> (
    PresenceImpacts,
    BoxStream<'static, Result<TransportEvent, TransportError>>,
) {
    let (sender, receiver) = unbounded_channel();
    let stream = futures_util::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|event| (event, receiver))
    })
    .boxed();
    (sender, stream)
}

pub(crate) async fn bind_and_seed<P: Principal>(
    nats: &Nats,
    bucket_name: &str,
    lanes: &[Arc<dyn PresenceLane<P>>],
) -> Result<KvBucket<Value>, EngineError> {
    let bucket = nats
        .bind_ephemeral::<Value>(bucket_name)
        .await
        .map_err(|error| {
            EngineError::Posture(format!("presence bucket {bucket_name} unusable: {error}"))
        })?;
    let max_age = bucket
        .max_age()
        .await
        .map_err(|error| EngineError::Service(Box::new(error)))?;
    for lane in lanes {
        if max_age < lane.ttl() {
            return Err(EngineError::Posture(format!(
                "presence bucket {bucket_name} caps values at {max_age:?}, shorter than the \
                 {ttl:?} lifetime the {projector} lane declares, so the per-key TTL would be \
                 truncated and the value would vanish before its presence ends",
                ttl = lane.ttl(),
                projector = lane.projector_name(),
            )));
        }
    }
    reseed(&bucket, lanes).await?;
    Ok(bucket)
}

async fn reseed<P: Principal>(
    bucket: &KvBucket<Value>,
    lanes: &[Arc<dyn PresenceLane<P>>],
) -> Result<(), EngineError> {
    let entries = bucket
        .all()
        .await
        .map_err(|error| EngineError::Service(Box::new(error)))?;
    for lane in lanes {
        lane.reseed(&entries)?;
    }
    Ok(())
}

const RECONNECT_POLL: std::time::Duration = std::time::Duration::from_millis(200);

fn emit_reset<P: Principal>(lanes: &[Arc<dyn PresenceLane<P>>], impacts: &PresenceImpacts) {
    let resets = lanes
        .iter()
        .map(|lane| Impact::projector_reset(lane.projector_name()))
        .collect();
    let _ = impacts.send(Ok(TransportEvent::Impacts(resets)));
}

pub(crate) async fn run_watch<P: Principal>(
    nats: Nats,
    bucket: KvBucket<Value>,
    lanes: Vec<Arc<dyn PresenceLane<P>>>,
    impacts: PresenceImpacts,
    shutdown: Arc<Stop>,
) {
    let stopping = shutdown.stopped();
    tokio::pin!(stopping);
    let mut was_down = false;
    let mut reseed_pending = false;
    loop {
        let mut watch = match bucket.watch_all().await {
            Ok(watch) => watch,
            Err(error) => {
                tracing::warn!(error = %crate::chain::describe(&error), "the presence watch could not be opened; retrying");
                tokio::select! {
                    () = &mut stopping => return,
                    () = tokio::time::sleep(RECONNECT_POLL) => continue,
                }
            }
        };
        let mut tick = tokio::time::interval(RECONNECT_POLL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if reseed_pending && nats.reachable() {
                match reseed(&bucket, &lanes).await {
                    Ok(()) => {
                        emit_reset(&lanes, &impacts);
                        reseed_pending = false;
                    }
                    Err(error) => {
                        tracing::warn!(
                            error = %crate::chain::describe(&error),
                            "reseeding presence after a reconnect failed; sessions keep their \
                             last values until a later reseed repairs them"
                        );
                    }
                }
            }
            let event = tokio::select! {
                () = &mut stopping => return,
                _ = tick.tick() => {
                    if nats.reachable() {
                        if was_down {
                            was_down = false;
                            reseed_pending = true;
                        }
                    } else {
                        was_down = true;
                    }
                    continue;
                }
                event = watch.next::<Value>() => event,
            };
            match event {
                None => {
                    reseed_pending = true;
                    break;
                }
                Some(Err(error)) => {
                    tracing::warn!(error = %crate::chain::describe(&error), "the presence watch dropped; re-establishing");
                    reseed_pending = true;
                    break;
                }
                Some(Ok(event)) => {
                    if let Some(impact) = fold(&lanes, event) {
                        let _ = impacts.send(Ok(TransportEvent::Impacts(vec![impact])));
                    }
                }
            }
        }
    }
}

fn fold<P: Principal>(lanes: &[Arc<dyn PresenceLane<P>>], event: KvEvent<Value>) -> Option<Impact> {
    let outcome = match &event {
        KvEvent::Put { key, value, .. } => lanes.iter().find_map(|lane| lane.fold_put(key, value)),
        KvEvent::Delete { key, .. } => lanes.iter().find_map(|lane| lane.fold_delete(key)),
    };
    match outcome {
        Some(Ok(impact)) => Some(impact),
        Some(Err(error)) => {
            tracing::warn!(error = %crate::chain::describe(&error), "a presence value could not be folded; the key is skipped");
            None
        }
        None => None,
    }
}
