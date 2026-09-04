pub mod flush;
pub(crate) mod guard;
pub(crate) mod persisted;
pub mod reader;
pub mod runtime;
pub mod seal;
pub(crate) mod single_flight;

use std::any::TypeId;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use futures_util::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::erase::ErasedAccumulator;
use crate::error::EngineError;
use crate::name::{AccumulatorName, NounName};
use crate::wire::Noun;

pub use flush::FlushOutcome;
pub use reader::ChunkReader;
pub use runtime::AccumulatorRuntime;
pub use seal::{SealMarker, Swept};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ChunkSeq(u64);

impl ChunkSeq {
    pub const ZERO: Self = Self(0);

    pub const MAX: u64 = i64::MAX as u64;

    pub fn new(value: u64) -> Result<Self, EngineError> {
        if value > Self::MAX {
            return Err(EngineError::ChunkSeqOutOfRange {
                seq: value,
                max: Self::MAX,
            });
        }
        Ok(Self(value))
    }

    pub(crate) const fn from_storable(value: i64) -> Self {
        Self(value as u64)
    }

    pub(crate) fn to_i64(self) -> i64 {
        i64::try_from(self.0).unwrap_or(i64::MAX)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub const fn follows(self, previous: Self) -> bool {
        self.0 == previous.0 + 1
    }
}

impl<'de> serde::Deserialize<'de> for ChunkSeq {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

impl std::fmt::Display for ChunkSeq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

pub trait Accumulator: Send + Sync + 'static {
    type Noun: Noun;
    type Chunk: Serialize + DeserializeOwned + Send;
    type State: Default + Clone + Send + Sync + 'static;

    fn name(&self) -> AccumulatorName;
    fn fold(&self, state: &mut Self::State, seq: ChunkSeq, chunk: Self::Chunk);
}

pub struct Durable(BoxFuture<'static, Result<(), EngineError>>);

impl Durable {
    pub fn new(flush: BoxFuture<'static, Result<(), EngineError>>) -> Self {
        Self(flush)
    }
}

impl Future for Durable {
    type Output = Result<(), EngineError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl std::fmt::Debug for Durable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Durable(<pending flush>)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Accumulated<S> {
    pub state: S,
    pub contiguous_to: Option<ChunkSeq>,
    pub gap: bool,
}

impl<S: Default> Default for Accumulated<S> {
    fn default() -> Self {
        Self {
            state: S::default(),
            contiguous_to: None,
            gap: false,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Registered {
    pub name: AccumulatorName,
    pub noun: NounName,
    pub erased: Arc<dyn ErasedAccumulator>,
}

pub(crate) type Registry = Arc<RwLock<HashMap<TypeId, Registered>>>;

pub(crate) fn new_registry() -> Registry {
    Arc::new(RwLock::new(HashMap::new()))
}

pub(crate) fn lookup<A: Accumulator>(registry: &Registry) -> Result<Registered, EngineError> {
    registry
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&TypeId::of::<A>())
        .cloned()
        .ok_or(EngineError::UnregisteredAccumulator(
            std::any::type_name::<A>(),
        ))
}

pub(crate) fn enroll<A: Accumulator>(
    registry: &Registry,
    accumulator: A,
) -> Result<(), EngineError> {
    let name = accumulator.name();
    let mut held = registry
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let taken = held
        .iter()
        .any(|(type_id, entry)| entry.name == name && *type_id != TypeId::of::<A>());
    if taken {
        return Err(EngineError::DuplicateAccumulatorName { name });
    }
    let entry = Registered {
        name,
        noun: <A::Noun as Noun>::NAME,
        erased: crate::erase::erase_accumulator(accumulator),
    };
    held.insert(TypeId::of::<A>(), entry);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_chunk_sequence_starts_at_zero_and_knows_its_successor() {
        assert_eq!(ChunkSeq::ZERO.get(), 0);
        assert!(ChunkSeq::new(1).unwrap().follows(ChunkSeq::ZERO));
        assert!(!ChunkSeq::new(2).unwrap().follows(ChunkSeq::ZERO));
        assert_eq!(ChunkSeq::ZERO.next(), ChunkSeq::new(1).unwrap());
    }

    #[test]
    fn a_chunk_sequence_a_bigint_cannot_store_faithfully_is_refused_at_construction() {
        let largest = ChunkSeq::new(ChunkSeq::MAX).expect("i64::MAX is storable");
        assert_eq!(largest.get(), i64::MAX as u64);
        assert_eq!(
            largest.to_i64(),
            i64::MAX,
            "the boundary encodes without wrapping"
        );
        let over = ChunkSeq::new(ChunkSeq::MAX + 1);
        assert!(matches!(
            over,
            Err(EngineError::ChunkSeqOutOfRange { seq, max })
                if seq == (i64::MAX as u64) + 1 && max == i64::MAX as u64
        ));
        let deserialized: Result<ChunkSeq, _> =
            serde_json::from_str(&format!("{}", (i64::MAX as u64) + 1));
        assert!(
            deserialized.is_err(),
            "an out-of-range sequence cannot be smuggled in through deserialization either"
        );
    }

    #[test]
    fn a_fresh_accumulated_state_reports_no_contiguous_prefix_and_no_gap() {
        let accumulated = Accumulated::<Vec<u8>>::default();
        assert!(accumulated.state.is_empty());
        assert_eq!(accumulated.contiguous_to, None);
        assert!(!accumulated.gap);
    }

    #[tokio::test]
    async fn a_durable_receipt_resolves_with_the_outcome_of_its_own_flush() {
        let ok = Durable::new(Box::pin(async { Ok(()) }));
        assert!(ok.await.is_ok());
        let failed = Durable::new(Box::pin(async {
            Err(EngineError::Config("flush lost".into()))
        }));
        assert!(failed.await.is_err());
    }

    struct Tokens;

    struct TokenNoun;

    impl Noun for TokenNoun {
        type Key = String;
        const NAME: NounName = NounName::from_static("token_stream");
    }

    impl Accumulator for Tokens {
        type Noun = TokenNoun;
        type Chunk = String;
        type State = String;

        fn name(&self) -> AccumulatorName {
            AccumulatorName::from_static("tokens")
        }

        fn fold(&self, state: &mut String, _seq: ChunkSeq, chunk: String) {
            state.push_str(&chunk);
        }
    }

    #[test]
    fn an_accumulator_that_was_never_registered_is_reported_by_its_rust_type() {
        let registry = new_registry();
        let miss = lookup::<Tokens>(&registry);
        assert!(matches!(
            miss,
            Err(EngineError::UnregisteredAccumulator(name)) if name.ends_with("Tokens")
        ));
        enroll(&registry, Tokens).expect("the first accumulator enrolls");
        let entry = lookup::<Tokens>(&registry).expect("the enrolled accumulator resolves");
        assert_eq!(entry.name.as_str(), "tokens");
        assert_eq!(entry.noun.as_str(), "token_stream");
        enroll(&registry, Tokens).expect("re-enrolling the same type replaces it");
    }

    struct Impostor;

    impl Accumulator for Impostor {
        type Noun = TokenNoun;
        type Chunk = String;
        type State = String;

        fn name(&self) -> AccumulatorName {
            AccumulatorName::from_static("tokens")
        }

        fn fold(&self, state: &mut String, _seq: ChunkSeq, chunk: String) {
            state.push_str(&chunk);
        }
    }

    #[test]
    fn two_accumulator_types_claiming_one_name_would_share_rows_so_the_second_is_refused() {
        let registry = new_registry();
        enroll(&registry, Tokens).expect("the first accumulator enrolls");
        let collision = enroll(&registry, Impostor);
        assert!(matches!(
            collision,
            Err(EngineError::DuplicateAccumulatorName { name }) if name.as_str() == "tokens"
        ));
        assert!(lookup::<Impostor>(&registry).is_err());
    }
}
