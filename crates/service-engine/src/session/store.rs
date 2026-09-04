use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::impact::Impact;
use crate::principal::{Principal, PrincipalId};
use crate::session::SessionId;
use crate::session::live::Session;
use crate::session::stream::DropList;

pub(crate) struct SessionTable<P: Principal> {
    sessions: BTreeMap<SessionId, Session<P>>,
}

impl<P: Principal> SessionTable<P> {
    pub(crate) fn new() -> Self {
        Self {
            sessions: BTreeMap::new(),
        }
    }

    pub(crate) fn insert(&mut self, session: Session<P>) {
        self.sessions.insert(session.id, session);
    }

    pub(crate) fn get(&self, id: SessionId) -> Option<&Session<P>> {
        self.sessions.get(&id)
    }

    pub(crate) fn get_mut(&mut self, id: SessionId) -> Option<&mut Session<P>> {
        self.sessions.get_mut(&id)
    }

    pub(crate) fn remove(&mut self, id: SessionId) -> Option<Session<P>> {
        self.sessions.remove(&id)
    }

    pub(crate) fn live_ids(&self) -> Vec<SessionId> {
        self.sessions
            .values()
            .filter(|s| s.is_live())
            .map(|s| s.id)
            .collect()
    }

    pub(crate) fn pending_count(&self) -> usize {
        self.sessions.values().filter(|s| s.is_pending()).count()
    }

    pub(crate) fn mark_pending_overflowed(&mut self) {
        for session in self.sessions.values_mut() {
            session.mark_overflowed_if_pending();
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Session<P>> {
        self.sessions.values()
    }

    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = &mut Session<P>> {
        self.sessions.values_mut()
    }

    pub(crate) fn hold_for_pending(&mut self, impacts: &[Impact], cap: usize) {
        for session in self.sessions.values_mut() {
            session.hold(impacts, cap);
        }
    }

    pub(crate) fn principal_of(&self, principal: PrincipalId) -> Option<P> {
        self.sessions
            .values()
            .find(|s| s.principal.id() == principal)
            .map(|s| s.principal.clone())
    }

    pub(crate) fn sessions_of(&self, principal: PrincipalId) -> Vec<SessionId> {
        self.sessions
            .values()
            .filter(|s| s.principal.id() == principal)
            .map(|s| s.id)
            .collect()
    }

    pub(crate) fn replace_principal(&mut self, principal: PrincipalId, next: P) {
        for session in self.sessions.values_mut() {
            if session.principal.id() == principal {
                session.principal = next.clone();
            }
        }
    }

    pub(crate) fn end_principal(&mut self, principal: PrincipalId) -> Vec<SessionId> {
        let ended = self.sessions_of(principal);
        let mut abandoned = Vec::new();
        for id in &ended {
            if let Some(session) = self.sessions.get_mut(id) {
                let connecting = session.is_pending();
                session.end();
                if !connecting {
                    abandoned.push(*id);
                }
            }
        }
        for id in &abandoned {
            self.sessions.remove(id);
        }
        ended
    }

    pub(crate) fn reap_dropped(&mut self, dropped: &DropList) -> usize {
        let taken: Vec<SessionId> = std::mem::take(
            &mut *dropped
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        let mut reaped = 0;
        for id in taken {
            if let Some(mut session) = self.sessions.remove(&id) {
                session.end();
                reaped += 1;
            }
        }
        reaped
    }

    pub(crate) fn reap_expired(&mut self, ttl: Duration) -> usize {
        let now = Instant::now();
        let expired: Vec<SessionId> = self
            .sessions
            .values()
            .filter(|s| !s.is_live() && now.duration_since(s.touched_at) > ttl)
            .map(|s| s.id)
            .collect();
        for id in &expired {
            if let Some(mut session) = self.sessions.remove(id) {
                session.end();
            }
        }
        expired.len()
    }
}
