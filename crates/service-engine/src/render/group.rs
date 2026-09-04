use std::collections::BTreeMap;
use std::sync::Arc;

use sqlx::PgPool;

use crate::accumulator::ChunkReader;
use crate::cohort::CohortKey;
use crate::erase::{ErasedLoadScope, ErasedProjector};
use crate::error::EngineError;
use crate::principal::{Principal, RlsApplier};
use crate::wire::{KeyBytes, ViewBytes};

pub(crate) type Rendered = BTreeMap<KeyBytes, Option<ViewBytes>>;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RenderCost {
    pub(crate) loads: usize,
    pub(crate) projections: usize,
}

pub(crate) struct Renderer<'a, P: Principal> {
    pub(crate) pg: &'a PgPool,
    pub(crate) chunks: &'a ChunkReader,
    pub(crate) rls: Option<&'a Arc<dyn RlsApplier<P>>>,
}

impl<P: Principal> Renderer<'_, P> {
    pub(crate) async fn render(
        &self,
        projector: &Arc<dyn ErasedProjector<P>>,
        under_rls: bool,
        cohort: CohortKey,
        principal: &P,
        keys: &[KeyBytes],
    ) -> Result<(Rendered, RenderCost), EngineError> {
        if keys.is_empty() {
            return Ok((Rendered::new(), RenderCost::default()));
        }
        let facts = if under_rls {
            let applier = self.rls.ok_or_else(|| {
                EngineError::Config("an RLS window has no RlsApplier".to_string())
            })?;
            let mut tx = self.pg.begin().await?;
            applier.apply(&mut tx, principal).await?;
            let facts = projector
                .load(ErasedLoadScope::PerPrincipal {
                    conn: &mut tx,
                    keys,
                    principal,
                    chunks: self.chunks,
                })
                .await?;
            tx.rollback().await?;
            facts
        } else {
            let cohorts = [(cohort, principal)];
            projector
                .load(ErasedLoadScope::Bulk {
                    pg: self.pg,
                    keys,
                    cohorts: &cohorts,
                    chunks: self.chunks,
                })
                .await?
        };
        let mut rendered = Rendered::new();
        for key in keys {
            rendered.insert(key.clone(), projector.project(&facts, key, principal)?);
        }
        Ok((
            rendered,
            RenderCost {
                loads: 1,
                projections: keys.len(),
            },
        ))
    }
}
