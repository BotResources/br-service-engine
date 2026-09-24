use crate::error::AccumulatorError;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use sqlx::{PgPool, Row};

use crate::accumulator::single_flight::KeyedGate;
use crate::accumulator::{Accumulated, Accumulator, ChunkSeq, Registered, Registry, lookup};
use crate::config::DEFAULT_FOLD_CACHE_CAPACITY;
use crate::dyn_compat::ErasedState;
use crate::error::EngineError;
use crate::name::AccumulatorName;
use crate::wire::{KeyBytes, Noun, encode_key};

type FoldKey = (AccumulatorName, KeyBytes);

struct CachedFold {
    state: ErasedState,
    contiguous_to: Option<ChunkSeq>,
    last_used: u64,
}

#[derive(Clone)]
pub struct ChunkReader {
    pg: PgPool,
    registry: Registry,
    folds: Arc<Mutex<HashMap<FoldKey, CachedFold>>>,
    gate: Arc<KeyedGate<FoldKey>>,
    clock: Arc<AtomicU64>,
    capacity: usize,
}

impl std::fmt::Debug for ChunkReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ChunkReader")
    }
}

impl ChunkReader {
    pub(crate) fn with_registry(pg: PgPool, registry: Registry) -> Self {
        Self {
            pg,
            registry,
            folds: Arc::new(Mutex::new(HashMap::new())),
            gate: Arc::new(KeyedGate::new()),
            clock: Arc::new(AtomicU64::new(0)),
            capacity: DEFAULT_FOLD_CACHE_CAPACITY,
        }
    }

    pub(crate) fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity.max(1);
    }

    pub(crate) fn pool(&self) -> &PgPool {
        &self.pg
    }

    #[cfg(test)]
    pub(crate) fn cached_folds(&self) -> usize {
        self.folds.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    pub async fn state<A: Accumulator>(
        &self,
        key: &<A::Noun as Noun>::Key,
    ) -> Result<Accumulated<A::State>, EngineError> {
        let entry = lookup::<A>(&self.registry)?;
        let key = encode_key::<A::Noun>(key)?;
        self.accumulate::<A>(&entry, key).await
    }

    pub async fn replay_verified<A: Accumulator>(
        &self,
        key: &<A::Noun as Noun>::Key,
        last_seq: ChunkSeq,
    ) -> Result<(A::State, crate::accumulator::SealHash), EngineError> {
        let entry = lookup::<A>(&self.registry)?;
        let key = encode_key::<A::Noun>(key)?;
        let key_value = key.decode::<serde_json::Value>()?;
        let rows = sqlx::query(
            "SELECT seq, chunk FROM service_engine.accumulator_chunk \
             WHERE accumulator = $1 AND key = $2 AND seq >= 0 AND seq <= $3 ORDER BY seq",
        )
        .bind(entry.name.as_str())
        .bind(&key_value)
        .bind(last_seq.to_i64())
        .fetch_all(&self.pg)
        .await?;

        let mut state = entry.erased.init_state();
        let mut values: Vec<serde_json::Value> = Vec::with_capacity(rows.len());
        let mut expected = ChunkSeq::ZERO;
        for row in &rows {
            let seq = ChunkSeq::from_storable(row.get::<i64, _>("seq"));
            if seq != expected {
                return Err(EngineError::Accumulator(AccumulatorError::SealTruncated {
                    accumulator: entry.name.clone(),
                    last_seq: last_seq.get(),
                    contiguous_to: expected.to_i64() - 1,
                }));
            }
            let chunk = row.get::<serde_json::Value, _>("chunk");
            entry.erased.fold(&mut state, seq, &chunk)?;
            values.push(chunk);
            expected = seq.next();
        }
        if expected.to_i64() != last_seq.to_i64() + 1 {
            return Err(EngineError::Accumulator(AccumulatorError::SealTruncated {
                accumulator: entry.name.clone(),
                last_seq: last_seq.get(),
                contiguous_to: expected.to_i64() - 1,
            }));
        }
        let folded = state
            .downcast_ref::<A::State>()
            .ok_or_else(|| {
                EngineError::Accumulator(AccumulatorError::StateMismatch {
                    accumulator: entry.name.clone(),
                })
            })?
            .clone();
        let digest = crate::accumulator::SealHash::of_values(values.iter());
        Ok((folded, digest))
    }

    pub(crate) fn forget(&self, accumulator: &AccumulatorName, key: &KeyBytes) {
        self.folds
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&(accumulator.clone(), key.clone()));
    }

    async fn accumulate<A: Accumulator>(
        &self,
        entry: &Registered,
        key: KeyBytes,
    ) -> Result<Accumulated<A::State>, EngineError> {
        let fold_key = (entry.name.clone(), key.clone());
        self.gate
            .run(fold_key, self.accumulate_inner::<A>(entry, key))
            .await
    }

    async fn accumulate_inner<A: Accumulator>(
        &self,
        entry: &Registered,
        key: KeyBytes,
    ) -> Result<Accumulated<A::State>, EngineError> {
        let cached = self.take(&entry.name, &key);
        let cached_mark = cached.as_ref().and_then(|fold| fold.contiguous_to);
        let key_value = key.decode::<serde_json::Value>()?;

        let head = sqlx::query(
            "SELECT (SELECT high_water FROM service_engine.accumulator_seal \
                     WHERE accumulator = $1 AND key = $2) AS sealed_high_water, \
                    (SELECT count(*) FROM service_engine.accumulator_chunk \
                     WHERE accumulator = $1 AND key = $2 AND seq <= $3) AS prefix_rows",
        )
        .bind(entry.name.as_str())
        .bind(&key_value)
        .bind(floor(cached_mark))
        .fetch_one(&self.pg)
        .await?;

        if head.get::<Option<i64>, _>("sealed_high_water").is_some() {
            return Ok(Accumulated::default());
        }

        let prefix_backed = head.get::<Option<i64>, _>("prefix_rows").unwrap_or(0)
            == cached_mark.map(|m| m.to_i64() + 1).unwrap_or(0);
        let (mut state, mut mark) = if prefix_backed {
            (
                cached
                    .map(|fold| fold.state)
                    .unwrap_or_else(|| entry.erased.init_state()),
                cached_mark,
            )
        } else {
            (entry.erased.init_state(), None)
        };

        let rows = sqlx::query(
            "SELECT seq, chunk FROM service_engine.accumulator_chunk \
             WHERE accumulator = $1 AND key = $2 AND seq > $3 ORDER BY seq",
        )
        .bind(entry.name.as_str())
        .bind(&key_value)
        .bind(floor(mark))
        .fetch_all(&self.pg)
        .await?;

        let mut expected = mark.map(ChunkSeq::next).unwrap_or(ChunkSeq::ZERO);
        let mut gap = false;
        for row in &rows {
            let raw = row.get::<i64, _>("seq");
            if raw < 0 {
                gap = true;
                break;
            }
            let seq = ChunkSeq::from_storable(raw);
            if seq != expected {
                gap = true;
                break;
            }
            entry
                .erased
                .fold(&mut state, seq, &row.get::<serde_json::Value, _>("chunk"))?;
            mark = Some(seq);
            expected = seq.next();
        }

        let folded = state
            .downcast_ref::<A::State>()
            .ok_or_else(|| {
                EngineError::Accumulator(AccumulatorError::StateMismatch {
                    accumulator: entry.name.clone(),
                })
            })?
            .clone();
        self.store(entry.name.clone(), key, state, mark);
        Ok(Accumulated {
            state: folded,
            contiguous_to: mark,
            gap,
        })
    }

    fn take(&self, accumulator: &AccumulatorName, key: &KeyBytes) -> Option<CachedFold> {
        self.folds
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&(accumulator.clone(), key.clone()))
    }

    fn store(
        &self,
        accumulator: AccumulatorName,
        key: KeyBytes,
        state: ErasedState,
        contiguous_to: Option<ChunkSeq>,
    ) {
        let now = self.clock.fetch_add(1, Ordering::Relaxed);
        let mut folds = self.folds.lock().unwrap_or_else(|p| p.into_inner());
        match folds.get(&(accumulator.clone(), key.clone())) {
            Some(existing) if existing.contiguous_to >= contiguous_to => {}
            _ => {
                folds.insert(
                    (accumulator, key),
                    CachedFold {
                        state,
                        contiguous_to,
                        last_used: now,
                    },
                );
            }
        }
        while folds.len() > self.capacity {
            let Some(evict) = folds
                .iter()
                .min_by_key(|(_, fold)| fold.last_used)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            folds.remove(&evict);
        }
    }
}

fn floor(mark: Option<ChunkSeq>) -> i64 {
    mark.map(ChunkSeq::to_i64).unwrap_or(-1)
}

#[cfg(test)]
#[path = "reader_tests.rs"]
mod tests;
