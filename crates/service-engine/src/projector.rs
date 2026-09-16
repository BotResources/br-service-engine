use std::hash::Hash;

use futures_util::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::{PgConnection, PgPool};

use crate::accumulator::ChunkReader;
use crate::cohort::CohortKey;
use crate::error::EngineError;
use crate::impact::{ForeignKey, Impact};
use crate::name::{NounName, ProjectorName};
use crate::population::{Inverse, Population};
use crate::principal::Principal;
use crate::session::WindowParams;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Emission {
    Coalesced,
    PerImpact,
}

pub enum LoadScope<'a, K, P: Principal> {
    Bulk {
        pg: &'a PgPool,
        keys: &'a [K],
        cohorts: &'a [(CohortKey, &'a P)],
        chunks: &'a ChunkReader,
    },
    PerPrincipal {
        conn: &'a mut PgConnection,
        keys: &'a [K],
        principal: &'a P,
        chunks: &'a ChunkReader,
    },
}

impl<'a, K, P: Principal> LoadScope<'a, K, P> {
    pub fn keys(&self) -> &'a [K] {
        match self {
            Self::Bulk { keys, .. } => keys,
            Self::PerPrincipal { keys, .. } => keys,
        }
    }

    pub fn chunks(&self) -> &'a ChunkReader {
        match self {
            Self::Bulk { chunks, .. } => chunks,
            Self::PerPrincipal { chunks, .. } => chunks,
        }
    }
}

pub trait Projector: Send + Sync + 'static {
    type Principal: Principal;
    type Key: Clone + Ord + Hash + Send + Sync + Serialize + DeserializeOwned + 'static;
    type Facts: Send + Sync + 'static;
    type View: Clone + PartialEq + Serialize + Send + Sync + 'static;

    fn name(&self) -> ProjectorName;

    fn nouns(&self) -> &'static [NounName];

    fn renders_under_rls(&self) -> bool {
        false
    }

    fn populate<'a>(
        &'a self,
        pg: &'a PgPool,
        window: &'a WindowParams,
        principal: &'a Self::Principal,
    ) -> BoxFuture<'a, Result<Population<Self::Key>, EngineError>>;

    fn inverse(&self, foreign: &ForeignKey) -> Inverse<Self::Key>;

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Self::Key, Self::Principal>,
    ) -> BoxFuture<'a, Result<Self::Facts, EngineError>>;

    /// Render one key's view from the facts loaded for it.
    ///
    /// `Ok(Some(view))` delivers the view, `Ok(None)` withholds it (the key is
    /// absent from the facts or invisible to the principal, so the session sees
    /// a `Remove`). `Err` reports that the stored document could not be
    /// projected at all — a poison the pod cannot render around; the render pass
    /// dead-letters it with this projector as the source and repairs, then ends,
    /// the sessions it faulted, rather than panicking the whole pod.
    fn project(
        &self,
        facts: &Self::Facts,
        key: &Self::Key,
        principal: &Self::Principal,
    ) -> Result<Option<Self::View>, EngineError>;

    fn cohort(&self, principal: &Self::Principal) -> CohortKey {
        CohortKey::principal(principal.id())
    }

    fn reset_threshold(&self) -> Option<usize> {
        None
    }

    fn emission(&self, _impact: &Impact) -> Emission {
        Emission::Coalesced
    }
}
