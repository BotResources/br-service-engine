pub(crate) mod commit;
pub mod flush;
pub(crate) mod guard;
mod hash;
pub(crate) mod ingress;
pub(crate) mod persisted;
pub mod reader;
pub mod runtime;
pub mod seal;
mod seq;
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

use crate::dyn_compat::ErasedAccumulator;
use crate::error::EngineError;
use crate::name::{AccumulatorName, NounName};
use crate::wire::Noun;

pub use flush::FlushOutcome;
pub use hash::SealHash;
pub use reader::ChunkReader;
pub use runtime::AccumulatorRuntime;
pub use seal::{SealMarker, Swept};
pub use seq::ChunkSeq;

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

pub(crate) fn lookup_by_name(
    registry: &Registry,
    name: &AccumulatorName,
) -> Result<Registered, EngineError> {
    registry
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .values()
        .find(|entry| &entry.name == name)
        .cloned()
        .ok_or_else(|| EngineError::UnregisteredAccumulatorName { name: name.clone() })
}

pub(crate) fn registered_count(registry: &Registry) -> usize {
    registry
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .len()
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
        erased: crate::dyn_compat::erase_accumulator(accumulator),
    };
    held.insert(TypeId::of::<A>(), entry);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
