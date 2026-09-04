use crate::delta::{Delta, ErasedView};
use crate::name::ProjectorName;
use crate::principal::Principal;
use crate::session::live::Session;
use crate::wire::{Cause, KeyBytes, ViewBytes};

#[derive(Debug, Clone)]
pub(crate) enum Outgoing {
    Upsert {
        projector: ProjectorName,
        key: KeyBytes,
        view: ViewBytes,
        cause: Option<Cause>,
    },
    Remove {
        projector: ProjectorName,
        key: KeyBytes,
        cause: Option<Cause>,
    },
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Delivered {
    pub(crate) deltas: usize,
    pub(crate) discarded: usize,
    pub(crate) resets: usize,
    pub(crate) lagged: bool,
}

pub(crate) fn deliver<P: Principal>(
    session: &mut Session<P>,
    outgoing: Vec<Outgoing>,
) -> Delivered {
    let mut report = Delivered::default();
    let room = session
        .outbox
        .capacity()
        .saturating_sub(session.outbox.len());
    report.lagged = outgoing.len() > room;
    let resetting = report.lagged || session.reset_pending;
    for out in outgoing {
        let delta = match out {
            Outgoing::Upsert {
                projector,
                key,
                view,
                cause,
            } => {
                session
                    .last_sent
                    .insert((projector.clone(), key.clone()), view.clone());
                if resetting {
                    report.discarded += 1;
                    continue;
                }
                let revision = session.next_revision();
                Delta::Upsert {
                    view: ErasedView::new(projector, key, view),
                    revision,
                    cause,
                }
            }
            Outgoing::Remove {
                projector,
                key,
                cause,
            } => {
                session.last_sent.remove(&(projector.clone(), key.clone()));
                if resetting {
                    report.discarded += 1;
                    continue;
                }
                let revision = session.next_revision();
                Delta::Remove {
                    projector,
                    key,
                    revision,
                    cause,
                }
            }
        };
        if session.outbox.push(delta) {
            report.deltas += 1;
        } else {
            report.discarded += 1;
        }
    }
    if resetting {
        session.reset_now();
        report.resets = 1;
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::Delta;
    use crate::name::ProjectorName;
    use crate::session::SessionId;
    use crate::session::live::Session;
    use crate::session::stream::{DropList, Outbox, SessionStream};
    use crate::test_support::TestPrincipal;
    use futures_util::StreamExt;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    const PROJECTOR: ProjectorName = ProjectorName::from_static("assignments");

    fn session(capacity: usize) -> (Session<TestPrincipal>, Arc<Outbox>) {
        let outbox = Arc::new(Outbox::new(capacity));
        let mut session = Session::pending(
            SessionId::new(),
            TestPrincipal::new(),
            Vec::new(),
            outbox.clone(),
        );
        session.go_live();
        (session, outbox)
    }

    fn upserts(count: usize) -> Vec<Outgoing> {
        (0..count)
            .map(|n| Outgoing::Upsert {
                projector: PROJECTOR,
                key: KeyBytes::encode(&n).expect("a key encodes"),
                view: ViewBytes::encode(&n).expect("a view encodes"),
                cause: None,
            })
            .collect()
    }

    fn buffered(outbox: &Outbox) -> Vec<Delta> {
        let mut drained = Vec::new();
        while let Some(delta) = outbox.take_front() {
            drained.push(delta);
        }
        drained
    }

    #[test]
    fn a_pass_whose_deltas_cannot_all_fit_resets_instead_of_delivering_a_prefix() {
        let (mut session, outbox) = session(4);
        let report = deliver(&mut session, upserts(5));
        assert!(report.lagged);
        assert_eq!(report.deltas, 0, "a partial prefix is never delivered");
        assert_eq!(report.discarded, 5);
        assert_eq!(report.resets, 1);
        let frames = buffered(&outbox);
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            Delta::Reset { views, revision } => {
                assert_eq!(
                    revision.get(),
                    1,
                    "the Reset is the first frame the client sees"
                );
                assert_eq!(
                    views.len(),
                    5,
                    "a lag Reset carries exactly what last_sent holds"
                );
            }
            other => panic!("a lagging session receives a Reset, got {other:?}"),
        }
    }

    #[test]
    fn a_reset_that_replaces_unread_deltas_hands_back_the_revisions_they_burned() {
        let (mut session, outbox) = session(4);
        let first = deliver(&mut session, upserts(2));
        assert_eq!(first.deltas, 2);
        assert_eq!(session.revision.get(), 2);

        let second = deliver(&mut session, upserts(3));
        assert!(second.lagged);
        assert_eq!(second.deltas, 0);
        let frames = buffered(&outbox);
        assert_eq!(
            frames.len(),
            1,
            "the Reset replaces the two deltas nobody read"
        );
        assert_eq!(
            frames[0].revision().get(),
            1,
            "a client that read nothing sees the Reset at the revision the discarded deltas held"
        );
    }

    #[test]
    fn a_delta_a_session_that_ended_mid_pass_can_no_longer_take_is_never_counted_as_delivered() {
        let (mut session, outbox) = session(4);
        outbox.close();
        let report = deliver(&mut session, upserts(2));
        assert!(!report.lagged, "an ended session is not a lagging one");
        assert_eq!(
            report.deltas, 0,
            "a delta the outbox refused was delivered to nobody"
        );
        assert_eq!(report.discarded, 2);
    }

    #[tokio::test]
    async fn a_reset_follows_the_last_revision_the_client_actually_read() {
        let dropped: DropList = Arc::new(Mutex::new(Vec::new()));
        let (mut session, outbox) = session(4);
        deliver(&mut session, upserts(2));
        let mut stream = SessionStream::new(session.id, outbox.clone(), dropped);
        assert_eq!(stream.next().await.expect("a delta").revision().get(), 1);

        let report = deliver(&mut session, upserts(4));
        assert!(report.lagged);
        let reset = stream.next().await.expect("the lagging session is reset");
        assert_eq!(
            reset.revision().get(),
            2,
            "the Reset follows the revision the client read, not the one it never saw"
        );
        assert!(matches!(reset, Delta::Reset { .. }));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), stream.next())
                .await
                .is_err(),
            "a lag resets the session, it never ends the stream"
        );
    }
}
