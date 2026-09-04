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

    fn project(
        &self,
        facts: &Self::Facts,
        key: &Self::Key,
        principal: &Self::Principal,
    ) -> Option<Self::View>;

    fn cohort(&self, principal: &Self::Principal) -> CohortKey {
        CohortKey::principal(principal.id())
    }

    fn emission(&self, _impact: &Impact) -> Emission {
        Emission::Coalesced
    }
}
