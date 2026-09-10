use std::sync::Arc;

use futures_util::Stream;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
pub enum Lane {
    Accumulated,
    Presence,
}

#[derive(Debug, Clone, PartialEq, Eq, async_graphql::SimpleObject)]
pub struct LanesPaused {
    pub lanes: Vec<Lane>,
}

#[derive(Debug, Clone, PartialEq, Eq, async_graphql::SimpleObject)]
pub struct LanesResumed {
    pub lanes: Vec<Lane>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaneNotice {
    Paused(Vec<Lane>),
    Resumed(Vec<Lane>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LanePhase {
    Running,
    Paused,
}

#[derive(Clone)]
pub(crate) struct LaneChannel {
    tx: Arc<watch::Sender<LanePhase>>,
    lanes: Vec<Lane>,
}

impl LaneChannel {
    pub(crate) fn new(lanes: Vec<Lane>) -> Self {
        let (tx, _rx) = watch::channel(LanePhase::Running);
        Self {
            tx: Arc::new(tx),
            lanes,
        }
    }

    pub(crate) fn receiver(&self) -> watch::Receiver<LanePhase> {
        self.tx.subscribe()
    }

    pub(crate) fn lanes(&self) -> Vec<Lane> {
        self.lanes.clone()
    }

    pub(crate) fn has_session_lanes(&self) -> bool {
        !self.lanes.is_empty()
    }

    pub(crate) fn set_paused(&self, paused: bool) {
        let phase = if paused {
            LanePhase::Paused
        } else {
            LanePhase::Running
        };
        self.tx.send_if_modified(|current| {
            if *current == phase {
                false
            } else {
                *current = phase;
                true
            }
        });
    }
}

pub(crate) fn notices(
    receiver: watch::Receiver<LanePhase>,
    lanes: Vec<Lane>,
) -> impl Stream<Item = LaneNotice> {
    futures_util::stream::unfold(
        (receiver, lanes, false),
        |(mut receiver, lanes, started)| async move {
            if !started {
                let phase = *receiver.borrow_and_update();
                if matches!(phase, LanePhase::Paused) {
                    return Some((LaneNotice::Paused(lanes.clone()), (receiver, lanes, true)));
                }
            }
            if receiver.changed().await.is_err() {
                return None;
            }
            let phase = *receiver.borrow_and_update();
            let notice = match phase {
                LanePhase::Paused => LaneNotice::Paused(lanes.clone()),
                LanePhase::Running => LaneNotice::Resumed(lanes.clone()),
            };
            Some((notice, (receiver, lanes, true)))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn a_pause_then_resume_reaches_a_subscriber_as_two_notices() {
        let channel = LaneChannel::new(vec![Lane::Accumulated, Lane::Presence]);
        let mut stream = Box::pin(notices(channel.receiver(), channel.lanes()));
        channel.set_paused(true);
        assert_eq!(
            stream.next().await,
            Some(LaneNotice::Paused(vec![Lane::Accumulated, Lane::Presence]))
        );
        channel.set_paused(false);
        assert_eq!(
            stream.next().await,
            Some(LaneNotice::Resumed(vec![Lane::Accumulated, Lane::Presence]))
        );
    }

    #[tokio::test]
    async fn a_subscriber_that_attaches_while_already_paused_is_told_at_once() {
        let channel = LaneChannel::new(vec![Lane::Presence]);
        channel.set_paused(true);
        let mut stream = Box::pin(notices(channel.receiver(), channel.lanes()));
        assert_eq!(
            stream.next().await,
            Some(LaneNotice::Paused(vec![Lane::Presence])),
            "a session that subscribes during an outage learns the lane is paused immediately"
        );
    }
}
