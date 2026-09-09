use std::sync::Arc;

use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use serde_json::Value;
use tokio::sync::Notify;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use crate::error::{EngineError, TransportError};
use crate::impact::{Impact, TransportEvent};
use crate::nats::{KvBucket, KvEvent, Nats};
use crate::presence::lane::PresenceLane;
use crate::principal::Principal;

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

pub(crate) async fn run_watch<P: Principal>(
    bucket: KvBucket<Value>,
    lanes: Vec<Arc<dyn PresenceLane<P>>>,
    impacts: PresenceImpacts,
    shutdown: Arc<Notify>,
) {
    let stopping = shutdown.notified();
    tokio::pin!(stopping);
    stopping.as_mut().enable();
    let mut first_open = true;
    loop {
        let mut watch = match bucket.watch_all().await {
            Ok(watch) => watch,
            Err(error) => {
                tracing::warn!(%error, "the presence watch could not be opened; retrying");
                tokio::select! {
                    () = &mut stopping => return,
                    () = tokio::time::sleep(std::time::Duration::from_millis(200)) => continue,
                }
            }
        };
        if !first_open {
            match reseed(&bucket, &lanes).await {
                Ok(()) => {
                    let resets = lanes
                        .iter()
                        .map(|lane| Impact::projector_reset(lane.projector_name()))
                        .collect();
                    let _ = impacts.send(Ok(TransportEvent::Impacts(resets)));
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "reseeding presence after a reconnect failed; sessions keep their last \
                         values until the next write repairs them"
                    );
                }
            }
        }
        first_open = false;
        loop {
            let event = tokio::select! {
                () = &mut stopping => return,
                event = watch.next::<Value>() => event,
            };
            match event {
                None => break,
                Some(Err(error)) => {
                    tracing::warn!(%error, "the presence watch dropped; re-establishing");
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
            tracing::warn!(%error, "a presence value could not be folded; the key is skipped");
            None
        }
        None => None,
    }
}
