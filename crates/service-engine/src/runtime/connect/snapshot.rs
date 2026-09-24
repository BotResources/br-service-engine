use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

use crate::cohort::CohortKey;
use crate::dyn_compat::ErasedPopulation;
use crate::error::AttachError;
use crate::page::KeyCeiling;
use crate::principal::Principal;
use crate::render::group::{Rendered, Renderer};
use crate::runtime::SessionRuntime;
use crate::session::WindowSpec;
use crate::session::capacity::admit;
use crate::session::live::{WindowShape, members_of};
use crate::wire::KeyBytes;

pub(super) struct WindowSnapshot {
    pub(super) members: BTreeSet<KeyBytes>,
    pub(super) shape: WindowShape,
    pub(super) views: Rendered,
}

struct AdmittedWindow<'s> {
    spec: &'s WindowSpec,
    members: BTreeSet<KeyBytes>,
    shape: WindowShape,
}

impl<P: Principal> SessionRuntime<P> {
    pub(super) async fn snapshot(
        &self,
        principal: &P,
        specs: &[WindowSpec],
    ) -> Result<Vec<WindowSnapshot>, AttachError> {
        let admitted = self.admit_windows(principal, specs).await?;
        self.render_windows(principal, admitted).await
    }

    async fn admit_windows<'s>(
        &self,
        principal: &P,
        specs: &'s [WindowSpec],
    ) -> Result<Vec<AdmittedWindow<'s>>, AttachError> {
        let mut admitted = Vec::with_capacity(specs.len());
        for spec in specs {
            let projector = self
                .registry
                .projector(&spec.projector)
                .ok_or_else(|| AttachError::UnknownProjector(spec.projector.clone()))?;
            let population = projector
                .populate(
                    &self.pg,
                    &spec.params,
                    KeyCeiling::attach(self.config.window_capacity),
                    principal,
                )
                .await
                .map_err(|source| AttachError::Snapshot {
                    projector: spec.projector.clone(),
                    source,
                })?;
            self.counters.populates.fetch_add(1, Ordering::Relaxed);
            if let ErasedPopulation::Query(query) = &population
                && query.interest().is_empty()
            {
                return Err(AttachError::EmptyInterest {
                    projector: spec.projector.clone(),
                });
            }
            let members = members_of(&population);
            admit(&spec.projector, members.len(), self.config.window_capacity)?;
            admitted.push(AdmittedWindow {
                spec,
                members,
                shape: WindowShape::of(&population),
            });
        }
        Ok(admitted)
    }

    async fn render_windows(
        &self,
        principal: &P,
        admitted: Vec<AdmittedWindow<'_>>,
    ) -> Result<Vec<WindowSnapshot>, AttachError> {
        let dead_letters = self.dead_letters();
        let renderer = Renderer {
            pg: &self.pg,
            chunks: &self.chunks,
            rls: self.registry.rls(),
            dead_letters: Some(&dead_letters),
        };
        let mut snapshots = Vec::with_capacity(admitted.len());
        for AdmittedWindow {
            spec,
            members,
            shape,
        } in admitted
        {
            let projector = self
                .registry
                .projector(&spec.projector)
                .ok_or_else(|| AttachError::UnknownProjector(spec.projector.clone()))?;
            let under_rls = projector.renders_under_rls();
            let cohort = if under_rls {
                CohortKey::principal(principal.id())
            } else {
                projector.cohort(principal)
            };
            let keys: Vec<KeyBytes> = members.iter().cloned().collect();
            let (views, cost) = renderer
                .render(projector, under_rls, cohort, principal, &keys)
                .await
                .map_err(|source| AttachError::Snapshot {
                    projector: spec.projector.clone(),
                    source,
                })?;
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
