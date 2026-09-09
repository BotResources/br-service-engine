use std::sync::Arc;

use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::error::TransportError;
use crate::impact::TransportEvent;

type Item = Result<TransportEvent, TransportError>;

pub(super) fn receiver_stream(receiver: mpsc::Receiver<Item>) -> BoxStream<'static, Item> {
    futures_util::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    })
    .fuse()
    .boxed()
}

pub(super) async fn drain_listener(
    mut source: BoxStream<'static, Item>,
    sink: mpsc::Sender<Item>,
    stop: Arc<Notify>,
) {
    let stopping = stop.notified();
    tokio::pin!(stopping);
    stopping.as_mut().enable();
    let mut owe_marker = false;
    loop {
        if owe_marker {
            tokio::select! {
                biased;
                () = &mut stopping => break,
                permit = sink.reserve() => match permit {
                    Ok(permit) => {
                        permit.send(Ok(TransportEvent::Reconnected));
                        owe_marker = false;
                    }
                    Err(_) => break,
                },
                item = source.next() => match item {
                    Some(_) => {}
                    None => break,
                },
            }
            continue;
        }
        let item = tokio::select! {
            biased;
            () = &mut stopping => break,
            item = source.next() => item,
        };
        let Some(item) = item else { break };
        match sink.try_send(item) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => owe_marker = true,
            Err(TrySendError::Closed(_)) => break,
        }
    }
    if owe_marker {
        let _ = sink.send(Ok(TransportEvent::Reconnected)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::impact::{Dims, Impact};
    use crate::name::NounName;
    use crate::wire::KeyBytes;
    use uuid::Uuid;

    fn impact() -> Impact {
        Impact::ResourceChanged {
            noun: NounName::from_static("thing"),
            key: KeyBytes::encode(&Uuid::now_v7()).expect("a key encodes"),
            dims: Dims::EMPTY,
            cause: None,
        }
    }

    fn burst(count: usize) -> Vec<Item> {
        (0..count)
            .map(|_| Ok(TransportEvent::Impacts(vec![impact()])))
            .collect()
    }

    #[tokio::test]
    async fn a_burst_larger_than_the_channel_drops_the_overflow_and_signals_one_reconnect() {
        let (sink, mut receiver) = mpsc::channel(2);
        let source = futures_util::stream::iter(burst(50)).boxed();
        let stop = Arc::new(Notify::new());

        let drained = tokio::spawn(drain_listener(source, sink, stop));

        let mut impacts = 0usize;
        let mut reconnects = 0usize;
        while let Some(item) = receiver.recv().await {
            match item.expect("no transport error in this scenario") {
                TransportEvent::Impacts(_) => impacts += 1,
                TransportEvent::Reconnected => reconnects += 1,
            }
        }
        drained
            .await
            .expect("the drain task ends when the source is exhausted");

        assert!(
            impacts <= 2,
            "the channel holds at most its capacity of forwarded items, saw {impacts}"
        );
        assert!(
            impacts < 50,
            "a burst larger than the channel must have dropped some items, saw {impacts}"
        );
        assert_eq!(
            reconnects, 1,
            "an overflow collapses to exactly one Reconnected loss signal, saw {reconnects}"
        );
    }

    #[tokio::test]
    async fn a_stream_inside_the_channel_is_forwarded_untouched_with_no_loss_signal() {
        let (sink, mut receiver) = mpsc::channel(64);
        let source = futures_util::stream::iter(burst(8)).boxed();
        let stop = Arc::new(Notify::new());

        let drained = tokio::spawn(drain_listener(source, sink, stop));

        let mut impacts = 0usize;
        let mut reconnects = 0usize;
        while let Some(item) = receiver.recv().await {
            match item.expect("no transport error in this scenario") {
                TransportEvent::Impacts(_) => impacts += 1,
                TransportEvent::Reconnected => reconnects += 1,
            }
        }
        drained.await.expect("the drain task ends");

        assert_eq!(impacts, 8, "every item that fits is forwarded");
        assert_eq!(
            reconnects, 0,
            "a channel that never overflows signals no loss"
        );
    }

    #[tokio::test]
    async fn the_stop_signal_ends_the_drain_even_while_the_source_is_idle() {
        let (sink, _receiver) = mpsc::channel::<Item>(4);
        let source = futures_util::stream::pending().boxed();
        let stop = Arc::new(Notify::new());

        let drained = tokio::spawn(drain_listener(source, sink, stop.clone()));
        stop.notify_one();

        tokio::time::timeout(std::time::Duration::from_secs(5), drained)
            .await
            .expect("the drain task stops promptly on the stop signal")
            .expect("the drain task joins cleanly");
    }
}
