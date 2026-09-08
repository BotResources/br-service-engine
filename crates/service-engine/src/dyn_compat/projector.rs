use std::collections::BTreeSet;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use sqlx::{PgConnection, PgPool};

use crate::accumulator::ChunkReader;
use crate::cohort::CohortKey;
use crate::erase::ErasedFacts;
use crate::error::EngineError;
use crate::impact::{ForeignKey, Impact};
use crate::name::{NounName, ProjectorName};
use crate::population::{Interest, Inverse, Population};
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

#[derive(Debug, Clone)]
pub enum ErasedInverse {
    Keys(BTreeSet<KeyBytes>),
    Query(ErasedWindowQuery),
    None,
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

    fn cohort(&self, principal: &P) -> CohortKey;

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

fn erase_query<Pr: Projector>(
    query: crate::population::WindowQuery<Pr::Key>,
) -> Result<ErasedWindowQuery, EngineError> {
    let predicate = query.predicate().clone();
    let keys = query
        .keys()
        .iter()
        .map(KeyBytes::encode)
        .collect::<Result<BTreeSet<KeyBytes>, _>>()?;
    Ok(ErasedWindowQuery {
        interest: query.interest().clone(),
        predicate: Arc::new(move |key, impact| Ok(predicate(&key.decode::<Pr::Key>()?, impact))),
        keys,
        authoritative: query.authoritative(),
    })
}

fn erase_keys<Pr: Projector>(
    keys: impl IntoIterator<Item = Pr::Key>,
) -> Result<BTreeSet<KeyBytes>, EngineError> {
    keys.into_iter().map(|k| KeyBytes::encode(&k)).collect()
}

fn decode_keys<Pr: Projector>(keys: &[KeyBytes]) -> Result<Vec<Pr::Key>, EngineError> {
    keys.iter().map(|k| k.decode::<Pr::Key>()).collect()
}

impl<Pr: Projector> ErasedProjector<Pr::Principal> for ProjectorAdapter<Pr> {
    fn name(&self) -> ProjectorName {
        self.0.name()
    }

    fn nouns(&self) -> &'static [NounName] {
        self.0.nouns()
    }

    fn cohort(&self, principal: &Pr::Principal) -> CohortKey {
        self.0.cohort(principal)
    }

    fn emission(&self, impact: &Impact) -> Emission {
        self.0.emission(impact)
    }

    fn populate<'a>(
        &'a self,
        pg: &'a PgPool,
        window: &'a WindowParams,
        principal: &'a Pr::Principal,
    ) -> BoxFuture<'a, Result<ErasedPopulation, EngineError>> {
        Box::pin(async move {
            match self.0.populate(pg, window, principal).await? {
                Population::Keys(keys) => Ok(ErasedPopulation::Keys(erase_keys::<Pr>(keys)?)),
                Population::Ordered { keys, open_head } => {
                    let keys = keys
                        .iter()
                        .map(KeyBytes::encode)
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(ErasedPopulation::Ordered { keys, open_head })
                }
                Population::Query(query) => Ok(ErasedPopulation::Query(erase_query::<Pr>(query)?)),
            }
        })
    }

    fn inverse(&self, foreign: &ForeignKey) -> Result<ErasedInverse, EngineError> {
        match self.0.inverse(foreign) {
            Inverse::Keys(keys) => Ok(ErasedInverse::Keys(erase_keys::<Pr>(keys)?)),
            Inverse::Query(query) => Ok(ErasedInverse::Query(erase_query::<Pr>(query)?)),
            Inverse::None => Ok(ErasedInverse::None),
        }
    }

    fn load<'a>(
        &'a self,
        scope: ErasedLoadScope<'a, Pr::Principal>,
    ) -> BoxFuture<'a, Result<ErasedFacts, EngineError>> {
        Box::pin(async move {
            match scope {
                ErasedLoadScope::Bulk {
                    pg,
                    keys,
                    cohorts,
                    chunks,
                } => {
                    let decoded = decode_keys::<Pr>(keys)?;
                    let facts = self
                        .0
                        .load(LoadScope::Bulk {
                            pg,
                            keys: &decoded,
                            cohorts,
                            chunks,
                        })
                        .await?;
                    Ok(Box::new(facts) as ErasedFacts)
                }
                ErasedLoadScope::PerPrincipal {
                    conn,
                    keys,
                    principal,
                    chunks,
                } => {
                    let decoded = decode_keys::<Pr>(keys)?;
                    let facts = self
                        .0
                        .load(LoadScope::PerPrincipal {
                            conn,
                            keys: &decoded,
                            principal,
                            chunks,
                        })
                        .await?;
                    Ok(Box::new(facts) as ErasedFacts)
                }
            }
        })
    }

    fn project(
        &self,
        facts: &ErasedFacts,
        key: &KeyBytes,
        principal: &Pr::Principal,
    ) -> Result<Option<ViewBytes>, EngineError> {
        let facts =
            facts
                .downcast_ref::<Pr::Facts>()
                .ok_or_else(|| EngineError::FactsMismatch {
                    projector: self.0.name(),
                })?;
        let key = key.decode::<Pr::Key>()?;
        match self.0.project(facts, &key, principal) {
            Some(view) => Ok(Some(ViewBytes::encode(&view)?)),
            None => Ok(None),
        }
    }
}
