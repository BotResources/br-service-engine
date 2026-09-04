use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_util::Stream;
use futures_util::task::AtomicWaker;

use crate::delta::Delta;
use crate::session::SessionId;

pub(crate) type DropList = Arc<Mutex<Vec<SessionId>>>;

struct Queued {
    deltas: VecDeque<Delta>,
    closed: bool,
}

pub(crate) struct Outbox {
    capacity: usize,
    queued: Mutex<Queued>,
    waker: AtomicWaker,
}

impl Outbox {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            queued: Mutex::new(Queued {
                deltas: VecDeque::new(),
                closed: false,
            }),
            waker: AtomicWaker::new(),
        }
    }

    fn queued(&self) -> std::sync::MutexGuard<'_, Queued> {
        self.queued.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(crate) fn push(&self, delta: Delta) -> bool {
        let accepted = {
            let mut queued = self.queued();
            if queued.closed || queued.deltas.len() >= self.capacity {
                false
            } else {
                queued.deltas.push_back(delta);
                true
            }
        };
        if accepted {
            self.waker.wake();
        }
        accepted
    }

    #[cfg(test)]
    pub(crate) fn take_front(&self) -> Option<Delta> {
        self.queued().deltas.pop_front()
    }

    pub(crate) fn discard_buffered(&self) -> usize {
        let mut queued = self.queued();
        let dropped = queued.deltas.len();
        queued.deltas.clear();
        dropped
    }

    pub(crate) fn close(&self) {
        {
            let mut queued = self.queued();
            queued.closed = true;
        }
        self.waker.wake();
    }

    pub(crate) fn len(&self) -> usize {
        self.queued().deltas.len()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    fn poll(&self, cx: &Context<'_>) -> Poll<Option<Delta>> {
        if let Some(delta) = self.queued().deltas.pop_front() {
            return Poll::Ready(Some(delta));
        }
        self.waker.register(cx.waker());
        let mut queued = self.queued();
        match queued.deltas.pop_front() {
            Some(delta) => Poll::Ready(Some(delta)),
            None if queued.closed => Poll::Ready(None),
            None => Poll::Pending,
        }
    }
}

pub struct SessionStream {
    id: SessionId,
    outbox: Arc<Outbox>,
    dropped: DropList,
}

impl SessionStream {
    pub(crate) fn new(id: SessionId, outbox: Arc<Outbox>, dropped: DropList) -> Self {
        Self {
            id,
            outbox,
            dropped,
        }
    }

    pub fn id(&self) -> SessionId {
        self.id
    }

    pub fn buffered(&self) -> usize {
        self.outbox.len()
    }
}

impl std::fmt::Debug for SessionStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionStream")
            .field("id", &self.id)
            .field("buffered", &self.outbox.len())
            .finish()
    }
}

impl Stream for SessionStream {
    type Item = Delta;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Delta>> {
        self.outbox.poll(cx)
    }
}

impl Drop for SessionStream {
    fn drop(&mut self) {
        self.dropped
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::Revision;
    use futures_util::StreamExt;

    fn reset(revision: u64) -> Delta {
        Delta::Reset {
            views: Vec::new(),
            revision: (0..revision).fold(Revision::FIRST, |r, _| r.next()),
        }
    }

    #[test]
    fn an_outbox_refuses_a_delta_past_its_capacity_rather_than_growing() {
        let outbox = Outbox::new(2);
        assert!(outbox.push(reset(0)));
        assert!(outbox.push(reset(1)));
        assert!(!outbox.push(reset(2)));
        assert_eq!(outbox.len(), 2);
    }

    #[test]
    fn discarding_the_buffer_reports_what_the_client_will_never_see() {
        let outbox = Outbox::new(2);
        outbox.push(reset(0));
        outbox.push(reset(1));
        assert_eq!(outbox.discard_buffered(), 2);
        assert_eq!(outbox.len(), 0);
        assert!(
            outbox.push(reset(9)),
            "a Reset always fits the buffer it just replaced"
        );
        assert_eq!(outbox.queued().deltas[0].revision().get(), 10);
    }

    #[tokio::test]
    async fn a_stream_ends_when_the_engine_closes_it_and_never_before() {
        let dropped: DropList = Arc::new(Mutex::new(Vec::new()));
        let outbox = Arc::new(Outbox::new(4));
        let id = SessionId::new();
        let mut stream = SessionStream::new(id, outbox.clone(), dropped.clone());
        outbox.push(reset(0));
        assert_eq!(stream.next().await.map(|d| d.revision().get()), Some(1));
        let pending =
            tokio::time::timeout(std::time::Duration::from_millis(20), stream.next()).await;
        assert!(pending.is_err());
        outbox.close();
        assert!(stream.next().await.is_none());
        drop(stream);
        assert_eq!(
            dropped.lock().unwrap().as_slice(),
            &[id],
            "a dropped stream must tell the engine so the session is reaped"
        );
    }
}
