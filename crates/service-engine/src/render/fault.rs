use std::collections::{BTreeMap, BTreeSet};

use crate::chain::describe;
use crate::session::SessionId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionFault {
    pub session: SessionId,
    pub reason: String,
    pub repaired: bool,
}

#[derive(Debug, Default)]
pub(crate) struct Faults {
    reasons: BTreeMap<SessionId, String>,
}

impl Faults {
    pub(crate) fn record(&mut self, session: SessionId, error: &dyn std::error::Error) {
        let reason = describe(error);
        tracing::error!(%session, %reason, "a render pass faulted a session");
        self.reasons.entry(session).or_insert(reason);
    }

    pub(crate) fn mark(&mut self, session: SessionId, reason: &str) {
        self.reasons
            .entry(session)
            .or_insert_with(|| reason.to_string());
    }

    pub(crate) fn contains(&self, session: SessionId) -> bool {
        self.reasons.contains_key(&session)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.reasons.is_empty()
    }

    pub(crate) fn sessions(&self) -> Vec<SessionId> {
        self.reasons.keys().copied().collect()
    }

    pub(crate) fn into_report(self, repaired: &BTreeSet<SessionId>) -> Vec<SessionFault> {
        self.reasons
            .into_iter()
            .map(|(session, reason)| SessionFault {
                session,
                reason,
                repaired: repaired.contains(&session),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::EngineError;
    use crate::name::ProjectorName;

    #[test]
    fn a_fault_reason_names_the_whole_cause_chain_not_only_its_top_word() {
        let mut faults = Faults::default();
        let session = SessionId::new();
        faults.record(
            session,
            &EngineError::CauseRequired {
                projector: ProjectorName::from_static("assignments"),
            },
        );
        let reported = faults.into_report(&BTreeSet::new());
        assert_eq!(reported.len(), 1);
        assert!(
            reported[0].reason.contains("assignments"),
            "an operator must learn which projector faulted, got {}",
            reported[0].reason
        );
        assert!(!reported[0].repaired);
    }

    #[test]
    fn one_session_faulting_twice_keeps_the_first_reason_and_is_reported_once() {
        let mut faults = Faults::default();
        let session = SessionId::new();
        faults.record(session, &EngineError::MissingPrincipalResolver);
        faults.record(
            session,
            &EngineError::CauseRequired {
                projector: ProjectorName::from_static("assignments"),
            },
        );
        assert!(faults.contains(session));
        let reported = faults.into_report(&BTreeSet::from([session]));
        assert_eq!(reported.len(), 1);
        assert!(reported[0].reason.contains("principal resolver"));
        assert!(reported[0].repaired);
    }
}
