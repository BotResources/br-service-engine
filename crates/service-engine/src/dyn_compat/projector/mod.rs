use std::collections::BTreeSet;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use sqlx::{PgConnection, PgPool};

use crate::accumulator::ChunkReader;
use crate::cohort::CohortKey;
use crate::dyn_compat::ErasedFacts;
use crate::error::EngineError;
use crate::impact::{ForeignKey, Impact};
use crate::name::{NounName, ProjectorName};
use crate::population::{Interest, Inverse, InverseLookup, Population};
use crate::principal::Principal;
use crate::projector::{Emission, LoadScope, Projector};
use crate::session::WindowParams;
use crate::wire::{KeyBytes, ViewBytes};

pub type ErasedPredicate =
    Arc<dyn Fn(&KeyBytes, &Impact) -> Result<bool, EngineError> + Send + Sync>;

#[derive(Clone)]
pub struct ErasedWindowQuery {
    interest: Interest,
    predicate: ErasedPredicate,
    keys: BTreeSet<KeyBytes>,
    authoritative: bool,
}

impl ErasedWindowQuery {
    pub fn interest(&self) -> &Interest {
        &self.interest
    }

    pub fn predicate(&self) -> &ErasedPredicate {
        &self.predicate
    }

    pub fn keys(&self) -> &BTreeSet<KeyBytes> {
        &self.keys
    }

    pub fn authoritative(&self) -> bool {
        self.authoritative
    }
}

impl std::fmt::Debug for ErasedWindowQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErasedWindowQuery")
            .field("interest", &self.interest)
            .field("keys", &self.keys.len())
            .field("authoritative", &self.authoritative)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub enum ErasedPopulation {
    Keys(BTreeSet<KeyBytes>),
    Ordered {
        keys: Vec<KeyBytes>,
        open_head: bool,
    },
    Query(ErasedWindowQuery),
}

pub type ErasedLookup = Arc<
    dyn for<'a> Fn(
            &'a mut PgConnection,
            &'a ForeignKey,
        ) -> BoxFuture<'a, Result<BTreeSet<KeyBytes>, EngineError>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub enum ErasedInverse {
    Keys(BTreeSet<KeyBytes>),
    Query(ErasedWindowQuery),
    Lookup(ErasedLookup),
    None,
}

impl std::fmt::Debug for ErasedInverse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErasedInverse::Keys(keys) => f.debug_tuple("Keys").field(keys).finish(),
            ErasedInverse::Query(query) => f.debug_tuple("Query").field(query).finish(),
            ErasedInverse::Lookup(_) => f.debug_struct("Lookup").finish_non_exhaustive(),
            ErasedInverse::None => f.write_str("None"),
        }
    }
}

pub enum ErasedLoadScope<'a, P: Principal> {
    Bulk {
        pg: &'a PgPool,
        keys: &'a [KeyBytes],
        cohorts: &'a [(CohortKey, &'a P)],
        chunks: &'a ChunkReader,
    },
    PerPrincipal {
        conn: &'a mut PgConnection,
        keys: &'a [KeyBytes],
        principal: &'a P,
        chunks: &'a ChunkReader,
    },
}

pub trait ErasedProjector<P: Principal>: Send + Sync + 'static {
    fn name(&self) -> ProjectorName;

    fn nouns(&self) -> &'static [NounName];

    fn renders_under_rls(&self) -> bool;

    fn cohort(&self, principal: &P) -> CohortKey;

    fn reset_threshold(&self) -> Option<usize>;

    fn emission(&self, impact: &Impact) -> Emission;

    fn populate<'a>(
        &'a self,
        pg: &'a PgPool,
        window: &'a WindowParams,
        principal: &'a P,
    ) -> BoxFuture<'a, Result<ErasedPopulation, EngineError>>;

    fn inverse(&self, foreign: &ForeignKey) -> Result<ErasedInverse, EngineError>;

    fn load<'a>(
        &'a self,
        scope: ErasedLoadScope<'a, P>,
    ) -> BoxFuture<'a, Result<ErasedFacts, EngineError>>;

    fn project(
        &self,
        facts: &ErasedFacts,
        key: &KeyBytes,
        principal: &P,
    ) -> Result<Option<ViewBytes>, EngineError>;
}

pub struct ProjectorAdapter<Pr>(Pr);

impl<Pr> ProjectorAdapter<Pr> {
    pub fn new(projector: Pr) -> Self {
        Self(projector)
    }

    pub fn inner(&self) -> &Pr {
        &self.0
    }
}

pub fn erase_projector<Pr: Projector>(projector: Pr) -> Arc<dyn ErasedProjector<Pr::Principal>> {
    Arc::new(ProjectorAdapter::new(projector))
}

mod adapter;
