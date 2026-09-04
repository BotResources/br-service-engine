use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::cohort::CohortKey;
use crate::erase::ErasedPopulation;
use crate::error::{AttachError, EngineError};
use crate::principal::Principal;
use crate::render::group::{Rendered, Renderer};
use crate::render::pass::PassContext;
use crate::render::repair::resnapshot;
use crate::runtime::SessionRuntime;
use crate::session::live::{Held, Session, WindowShape, WindowState, members_of};
use crate::session::stream::Outbox;
use crate::session::{AttachRequest, SessionId, SessionStream, WindowSpec};
use crate::wire::KeyBytes;

struct WindowSnapshot {
    members: BTreeSet<KeyBytes>,
    shape: WindowShape,
    views: Rendered,
}

impl<P: Principal> SessionRuntime<P> {
    pub async fn attach(&self, request: AttachRequest<P>) -> Result<SessionStream, AttachError> {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(AttachError::ShuttingDown);
        }
        if self.registry.resolver().is_none() {
            return Err(AttachError::MissingPrincipalResolver);
        }
        for spec in &request.windows {
            if self.registry.projector(&spec.projector).is_none() {
                return Err(AttachError::UnknownProjector(spec.projector.clone()));
            }
            if spec.rls && self.registry.rls().is_none() {
                return Err(AttachError::MissingRlsApplier {
                    projector: spec.projector.clone(),
                });
            }
        }
        let id = SessionId::new();
        let outbox = Arc::new(Outbox::new(self.config.session_buffer));
        let windows = request
            .windows
            .iter()
            .map(|spec| WindowState {
                projector: spec.projector.clone(),
                params: spec.params.clone(),
                rls: spec.rls,
                members: BTreeSet::new(),
                shape: WindowShape::Fixed,
            })
            .collect();
        self.table.lock().await.insert(Session::pending(
            id,
            request.principal.clone(),
            windows,
            outbox.clone(),
        ));

        let snapshots = match self.snapshot(&request.principal, &request.windows).await {
            Ok(snapshots) => snapshots,
            Err(refusal) => {
                self.table.lock().await.remove(id);
                return Err(refusal);
            }
        };

        let (held, stream) = {
            let mut table = self.table.lock().await;
            match table.get(id) {
                None => return Err(self.connect_gone()),
                Some(session) if !session.is_pending() => {
                    table.remove(id);
                    return Err(self.connect_lost());
                }
                Some(_) => {}
            }
            if self.shutting_down.load(Ordering::SeqCst) {
                table.remove(id);
                return Err(AttachError::ShuttingDown);
            }
            let session = table.get_mut(id).expect("the pending session is present");
            let mut seeded = Vec::new();
            for (index, snapshot) in snapshots.into_iter().enumerate() {
                let window = &mut session.windows[index];
                window.members = snapshot.members;
                window.shape = snapshot.shape;
                let projector = window.projector.clone();
                for (key, view) in snapshot.views {
                    if let Some(view) = view {
                        seeded.push(((projector.clone(), key), view));
                    }
                }
            }
            session.last_sent.extend(seeded);
            session.reset_now();
            let stream = SessionStream::new(id, outbox, self.dropped.clone());
            (session.go_live(), stream)
        };
        let opened = match held {
            Held::Replay(impacts) if impacts.is_empty() => Ok(()),
            Held::Replay(impacts) => self.render_session(id, impacts).await.map(|_| ()),
            Held::Overflowed => self.resnapshot_one(id).await,
        };
        match opened {
            Ok(()) => Ok(stream),
            Err(error) => {
                if let Some(mut session) = self.table.lock().await.remove(id) {
                    session.end();
                }
                drop(stream);
                Err(AttachError::HeldImpacts(error))
            }
        }
    }

    fn connect_lost(&self) -> AttachError {
        if self.shutting_down.load(Ordering::SeqCst) {
            AttachError::ShuttingDown
        } else {
            AttachError::PrincipalRevoked
        }
    }

    fn connect_gone(&self) -> AttachError {
        if self.shutting_down.load(Ordering::SeqCst) {
            AttachError::ShuttingDown
        } else {
            AttachError::ConnectTimedOut {
                after: self.config.session_ttl,
            }
        }
    }

    async fn resnapshot_one(&self, id: SessionId) -> Result<(), EngineError> {
        let mut table = self.table.lock().await;
        let ctx = PassContext {
            pg: &self.pg,
            registry: &self.registry,
            chunks: &self.chunks,
            config: &self.config,
        };
        let cost = resnapshot(&ctx, &mut table, id).await?;
        drop(table);
        self.counters.absorb_repair(&cost);
        self.counters.resets.fetch_add(1, Ordering::Relaxed);
        crate::observe::record_resets(1);
        Ok(())
    }

    pub async fn resnapshot_all(&self) -> Result<usize, EngineError> {
        let mut table = self.table.lock().await;
        table.mark_pending_overflowed();
        let ids = table.live_ids();
        let ctx = PassContext {
            pg: &self.pg,
            registry: &self.registry,
            chunks: &self.chunks,
            config: &self.config,
        };
        let mut reset = 0;
        for id in ids {
            match resnapshot(&ctx, &mut table, id).await {
                Ok(cost) => {
                    self.counters.absorb_repair(&cost);
                    if let Some(session) = table.get_mut(id) {
                        session.repair_pending = false;
                        session.repair_attempts = 0;
                    }
                    reset += 1;
                }
                Err(error) => {
                    let Some(session) = table.get_mut(id) else {
                        continue;
                    };
                    session.repair_attempts += 1;
                    let attempts = session.repair_attempts;
                    if attempts >= self.config.repair_attempts {
                        session.end();
                        tracing::error!(
                            %id,
                            attempts,
                            reason = %crate::chain::describe(&error),
                            "a session could not be re-snapshotted across repeated reconnect \
                             passes, so it is ended rather than left live on its pre-gap view"
                        );
                    } else {
                        session.repair_pending = true;
                        tracing::warn!(
                            %id,
                            attempts,
                            reason = %crate::chain::describe(&error),
                            "a reconnect resnapshot failed; the session holds its last good view \
                             and a subsequent pass retries the repair, never a Reset from stale \
                             state"
                        );
                    }
                }
            }
        }
        self.counters
            .resets
            .fetch_add(reset as u64, Ordering::Relaxed);
        crate::observe::record_resets(reset);
        Ok(reset)
    }

    async fn snapshot(
        &self,
        principal: &P,
        specs: &[WindowSpec],
    ) -> Result<Vec<WindowSnapshot>, AttachError> {
        let renderer = Renderer {
            pg: &self.pg,
            chunks: &self.chunks,
            rls: self.registry.rls(),
        };
        let mut snapshots = Vec::with_capacity(specs.len());
        for spec in specs {
            let projector = self
                .registry
                .projector(&spec.projector)
                .ok_or_else(|| AttachError::UnknownProjector(spec.projector.clone()))?;
            let refuse = |source| AttachError::Snapshot {
                projector: spec.projector.clone(),
                source,
            };
            let population = projector
                .populate(&self.pg, &spec.params, principal)
                .await
                .map_err(refuse)?;
            if let ErasedPopulation::Query(query) = &population
                && query.interest().is_empty()
            {
                return Err(AttachError::EmptyInterest {
                    projector: spec.projector.clone(),
                });
            }
            let members = members_of(&population);
            let shape = WindowShape::of(&population);
            let cohort = if spec.rls {
                CohortKey::principal(principal.id())
            } else {
                projector.cohort(principal)
            };
            let keys: Vec<KeyBytes> = members.iter().cloned().collect();
            let (views, cost) = renderer
                .render(projector, spec.rls, cohort, principal, &keys)
                .await
                .map_err(refuse)?;
            self.counters.populates.fetch_add(1, Ordering::Relaxed);
            self.counters
                .loads
                .fetch_add(cost.loads as u64, Ordering::Relaxed);
            self.counters
                .projections
                .fetch_add(cost.projections as u64, Ordering::Relaxed);
            snapshots.push(WindowSnapshot {
                members,
                shape,
                views,
            });
        }
        Ok(snapshots)
    }
}

#[cfg(test)]
mod tests;
