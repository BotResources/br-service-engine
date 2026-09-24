use super::*;

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

fn erase_lookup<Pr: Projector>(lookup: InverseLookup<Pr::Key>) -> ErasedLookup {
    Arc::new(move |conn, foreign| {
        let lookup = lookup.clone();
        Box::pin(async move {
            let keys = lookup(conn, foreign).await?;
            keys.iter()
                .map(KeyBytes::encode)
                .collect::<Result<BTreeSet<KeyBytes>, EngineError>>()
        })
    })
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

    fn renders_under_rls(&self) -> bool {
        self.0.renders_under_rls()
    }

    fn cohort(&self, principal: &Pr::Principal) -> CohortKey {
        self.0.cohort(principal)
    }

    fn reset_threshold(&self) -> Option<usize> {
        self.0.reset_threshold()
    }

    fn emission(&self, impact: &Impact) -> Emission {
        self.0.emission(impact)
    }

    fn populate<'a>(
        &'a self,
        pg: &'a PgPool,
        window: &'a WindowParams,
        ceiling: KeyCeiling,
        principal: &'a Pr::Principal,
    ) -> BoxFuture<'a, Result<ErasedPopulation, EngineError>> {
        Box::pin(async move {
            match self.0.populate(pg, window, ceiling, principal).await? {
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
            Inverse::Lookup(lookup) => Ok(ErasedInverse::Lookup(erase_lookup::<Pr>(lookup))),
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
        let decoded = key.decode::<Pr::Key>()?;
        match self.0.project(facts, &decoded, principal) {
            Ok(Some(view)) => Ok(Some(ViewBytes::encode(&view)?)),
            Ok(None) => Ok(None),
            Err(source) => Err(EngineError::Projection {
                projector: self.0.name(),
                key: String::from_utf8_lossy(key.as_slice()).into_owned(),
                source: Box::new(source),
            }),
        }
    }
}
