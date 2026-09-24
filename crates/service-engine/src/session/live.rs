use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use crate::delta::{Delta, ErasedView, Revision};
use crate::impact::Impact;
use crate::name::ProjectorName;
use crate::principal::Principal;
use crate::session::SessionId;
use crate::session::stream::Outbox;
use crate::wire::{KeyBytes, ViewBytes};

pub(crate) use crate::session::window::{WindowShape, WindowState, members_of, refreshed_members};

pub(crate) type ViewKey = (ProjectorName, KeyBytes);

pub(crate) enum Phase {
    Pending { held: Vec<Impact>, overflowed: bool },
    Live,
    Ended(Ending),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ending {
    Closed,
    PrincipalRevoked,
    PrincipalFaulted,
}

pub(crate) enum Held {
    Replay(Vec<Impact>),
    Overflowed,
}

pub(crate) struct Session<P: Principal> {
    pub(crate) id: SessionId,
    pub(crate) principal: P,
    pub(crate) windows: Vec<WindowState>,
    pub(crate) last_sent: BTreeMap<ViewKey, ViewBytes>,
    pub(crate) revision: Revision,
    pub(crate) phase: Phase,
    pub(crate) outbox: Arc<Outbox>,
    pub(crate) reset_pending: bool,
    pub(crate) repair_pending: bool,
    pub(crate) repair_attempts: u32,
    pub(crate) touched_at: Instant,
    pub(crate) attached_at: Instant,
}

impl<P: Principal> Session<P> {
    pub(crate) fn pending(
        id: SessionId,
        principal: P,
        windows: Vec<WindowState>,
        outbox: Arc<Outbox>,
    ) -> Self {
        Self {
            id,
            principal,
            windows,
            last_sent: BTreeMap::new(),
            revision: Revision::ZERO,
            phase: Phase::Pending {
                held: Vec::new(),
                overflowed: false,
            },
            outbox,
            reset_pending: false,
            repair_pending: false,
            repair_attempts: 0,
            touched_at: Instant::now(),
            attached_at: Instant::now(),
        }
    }

    pub(crate) fn is_live(&self) -> bool {
        matches!(self.phase, Phase::Live)
    }

    pub(crate) fn is_pending(&self) -> bool {
        matches!(self.phase, Phase::Pending { .. })
    }

    pub(crate) fn hold(&mut self, impacts: &[Impact], cap: usize) {
        let Phase::Pending { held, overflowed } = &mut self.phase else {
            return;
        };
        if *overflowed {
            return;
        }
        if held.len() + impacts.len() > cap {
            *overflowed = true;
            held.clear();
            return;
        }
        held.extend_from_slice(impacts);
    }

    pub(crate) fn mark_overflowed_if_pending(&mut self) {
        if let Phase::Pending { held, overflowed } = &mut self.phase {
            *overflowed = true;
            held.clear();
        }
    }

    pub(crate) fn go_live(&mut self) -> Held {
        let held = match std::mem::replace(&mut self.phase, Phase::Live) {
            Phase::Pending {
                overflowed: true, ..
            } => Held::Overflowed,
            Phase::Pending { held, .. } => Held::Replay(held),
            _ => Held::Replay(Vec::new()),
        };
        self.touched_at = Instant::now();
        held
    }

    pub(crate) fn holds(&self, projector: &ProjectorName, key: &KeyBytes) -> bool {
        self.windows
            .iter()
            .any(|w| &w.projector == projector && w.members.contains(key))
    }

    pub(crate) fn next_revision(&mut self) -> Revision {
        self.revision = self.revision.next();
        self.revision
    }

    pub(crate) fn snapshot_views(&self) -> Vec<ErasedView> {
        self.last_sent
            .iter()
            .map(|((projector, key), view)| {
                ErasedView::new(projector.clone(), key.clone(), view.clone())
            })
            .collect()
    }

    pub(crate) fn reset_frame(&mut self) -> Delta {
        let views = self.snapshot_views();
        Delta::Reset {
            views,
            revision: self.next_revision(),
        }
    }

    pub(crate) fn reset_now(&mut self) {
        let unread = self.outbox.discard_buffered();
        self.revision = self.revision.rewound(unread as u64);
        let frame = self.reset_frame();
        self.outbox.push(frame);
        self.reset_pending = false;
        self.touched_at = Instant::now();
    }

    pub(crate) fn ending(&self) -> Option<Ending> {
        match self.phase {
            Phase::Ended(ending) => Some(ending),
            Phase::Pending { .. } | Phase::Live => None,
        }
    }

    pub(crate) fn end(&mut self) {
        self.end_as(Ending::Closed);
    }

    pub(crate) fn end_as(&mut self, ending: Ending) {
        self.phase = Phase::Ended(ending);
        self.outbox.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::impact::Dims;
    use crate::session::SessionId;
    use crate::test_support::{Assignment, TestPrincipal};

    fn pending() -> Session<TestPrincipal> {
        Session::pending(
            SessionId::new(),
            TestPrincipal::new(),
            Vec::new(),
            Arc::new(Outbox::new(4)),
        )
    }

    fn impacts(count: usize) -> Vec<Impact> {
        (0..count)
            .map(|n| {
                Impact::resource::<Assignment>(&uuid::Uuid::from_u128(n as u128), Dims::EMPTY)
                    .expect("a key encodes")
            })
            .collect()
    }

    #[test]
    fn a_connection_replays_every_impact_it_held_while_it_fitted_the_bound() {
        let mut session = pending();
        session.hold(&impacts(2), 3);
        session.hold(&impacts(1), 3);
        match session.go_live() {
            Held::Replay(held) => assert_eq!(held.len(), 3),
            Held::Overflowed => panic!("a burst inside the bound is replayed, never upgraded"),
        }
    }

    #[test]
    fn a_burst_past_the_bound_is_upgraded_to_a_reset_instead_of_growing_the_pod() {
        let mut session = pending();
        session.hold(&impacts(2), 2);
        session.hold(&impacts(2), 2);
        session.hold(&impacts(2), 2);
        let Phase::Pending { held, overflowed } = &session.phase else {
            panic!("the session is still connecting");
        };
        assert!(overflowed);
        assert!(
            held.is_empty(),
            "an overflowed connection keeps no impact at all, so the pod stops growing"
        );
        assert!(matches!(session.go_live(), Held::Overflowed));
    }
}
