use std::collections::BTreeSet;

use crate::cohort::CohortKey;
use crate::error::EngineError;
use crate::name::ProjectorName;
use crate::principal::Principal;
use crate::render::deliver::{Outgoing, deliver};
use crate::render::group::Renderer;
use crate::runtime::SessionRuntime;
use crate::session::SessionId;
use crate::session::WindowParams;
use crate::session::live::members_of;
use crate::wire::KeyBytes;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PageReport {
    pub added: usize,
    pub released: usize,
    pub delivered: usize,
}

impl<P: Principal> SessionRuntime<P> {
    pub async fn page(
        &self,
        caller: &P,
        session: SessionId,
        projector: ProjectorName,
        cursor: WindowParams,
    ) -> Result<PageReport, EngineError> {
        let principal = {
            let table = self.table.lock().await;
            let live = table
                .get(session)
                .filter(|s| s.is_live() && s.principal.id() == caller.id())
                .ok_or(EngineError::NoLiveSession { session })?;
            if !live.windows.iter().any(|w| w.projector == projector) {
                return Err(EngineError::NoSuchWindow {
                    session,
                    projector: projector.clone(),
                });
            }
            live.principal.clone()
        };
        let erased = self
            .registry
            .projector(&projector)
            .ok_or_else(|| EngineError::UnboundProjector(projector.clone()))?
            .clone();
        let population = erased.populate(&self.pg, &cursor, &principal).await?;
        let page_keys = members_of(&population);

        let (added, released) = {
            let mut table = self.table.lock().await;
            let live = table
                .get_mut(session)
                .filter(|s| s.is_live())
                .ok_or(EngineError::NoLiveSession { session })?;
            let index = live
                .windows
                .iter()
                .position(|w| w.projector == projector)
                .ok_or_else(|| EngineError::NoSuchWindow {
                    session,
                    projector: projector.clone(),
                })?;
            let added: BTreeSet<KeyBytes> = page_keys
                .difference(&live.windows[index].members)
                .cloned()
                .collect();
            let window = &mut live.windows[index];
            if !added.is_empty() {
                window.members.extend(added.iter().cloned());
                window.pages.push(added.clone());
            }
            let released = window.evict_to_capacity(self.config.window_capacity);
            for key in &released {
                live.last_sent.remove(&(projector.clone(), key.clone()));
            }
            (added, released)
        };

        let mut report = PageReport {
            added: added.len(),
            released: released.len(),
            delivered: 0,
        };
        let keys: Vec<KeyBytes> = added.into_iter().collect();
        if keys.is_empty() {
            return Ok(report);
        }

        let under_rls = erased.renders_under_rls();
        let cohort = if under_rls {
            CohortKey::principal(principal.id())
        } else {
            erased.cohort(&principal)
        };
        let dead_letters = self.dead_letters();
        let renderer = Renderer {
            pg: &self.pg,
            chunks: &self.chunks,
            rls: self.registry.rls(),
            dead_letters: Some(&dead_letters),
        };
        let (rendered, _cost) = renderer
            .render(&erased, under_rls, cohort, &principal, &keys)
            .await?;

        let mut table = self.table.lock().await;
        let live = table
            .get_mut(session)
            .filter(|s| s.is_live())
            .ok_or(EngineError::NoLiveSession { session })?;
        let in_window = |key: &KeyBytes| {
            live.windows
                .iter()
                .any(|w| w.projector == projector && w.members.contains(key))
        };
        let mut outgoing = Vec::new();
        for key in &keys {
            if !in_window(key) {
                continue;
            }
            if live
                .last_sent
                .contains_key(&(projector.clone(), key.clone()))
            {
                continue;
            }
            if let Some(Some(view)) = rendered.get(key) {
                outgoing.push(Outgoing::Upsert {
                    projector: projector.clone(),
                    key: key.clone(),
                    view: view.clone(),
                    cause: None,
                });
            }
        }
        let delivered = deliver(live, outgoing);
        report.delivered = delivered.deltas;
        Ok(report)
    }
}
