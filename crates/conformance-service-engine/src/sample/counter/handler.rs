use br_core_integration::{Aggregate as CoordAggregate, Bc, CommandCoords, Verb};
use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::error::EngineError;
use service_engine::gate::Reason;
use service_engine::inbound::{Disposition, ReactionCoordinates, ReactionError, ReactionMessage};
use service_engine::persistence::{Aggregate, Persistence};
use service_engine::pipeline::{Mutation, MutationFault, MutationInput, Ops, Reaction};
use uuid::Uuid;

use crate::sample::counter::crud::CrudCounter;
use crate::sample::counter::event::CounterEvent;
use crate::sample::counter::full::FullCounter;
use crate::sample::counter::slice::CounterAggregate;
use crate::sample::counter::soft::SoftCounter;
use crate::sample::principal::SamplePrincipal;

pub struct Bump {
    pub key: Uuid,
    pub tenant: Uuid,
    pub amount: i64,
    pub author: String,
}

#[derive(Debug)]
pub enum CounterFault {
    Refused(Reason),
    Store(String),
}

impl std::fmt::Display for CounterFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(reason) => write!(f, "refused: {}", reason.code()),
            Self::Store(detail) => write!(f, "store: {detail}"),
        }
    }
}

impl MutationFault for CounterFault {
    fn reason(&self) -> Option<Reason> {
        match self {
            Self::Refused(reason) => Some(*reason),
            Self::Store(_) => None,
        }
    }
}

impl From<Reason> for CounterFault {
    fn from(reason: Reason) -> Self {
        Self::Refused(reason)
    }
}

impl From<EngineError> for CounterFault {
    fn from(error: EngineError) -> Self {
        Self::Store(error.to_string())
    }
}

async fn apply_bump<A>(cx: &mut Ops<'_>, input: &Bump) -> Result<(), CounterFault>
where
    A: CounterAggregate,
    <A as Aggregate>::Store: Persistence<Key = Uuid, Event = CounterEvent>,
{
    let mut aggregate = match cx.load::<A>(&input.key).await? {
        Some(aggregate) => aggregate,
        None => A::open(input.key, input.tenant),
    };
    aggregate
        .state_mut()
        .bump(input.amount, input.author.clone())?;
    cx.save(&aggregate).await?;
    cx.impact_caused::<A::Noun, _>(&input.key, "bumped")?;
    Ok(())
}

macro_rules! bump_input {
    ($name:ident, $handler:ident, $agg:ty, $lit:literal) => {
        #[derive(Debug, Deserialize)]
        pub struct $name {
            pub key: Uuid,
            pub tenant: Uuid,
            pub amount: i64,
            pub author: String,
        }

        impl MutationInput for $name {
            type Output = ();
            type Error = CounterFault;
            const NAME: &'static str = $lit;
        }

        impl From<$name> for Bump {
            fn from(input: $name) -> Bump {
                Bump {
                    key: input.key,
                    tenant: input.tenant,
                    amount: input.amount,
                    author: input.author,
                }
            }
        }

        pub fn $handler<'m>(
            cx: &'m mut Mutation<'m, SamplePrincipal>,
            input: $name,
        ) -> BoxFuture<'m, Result<(), CounterFault>> {
            Box::pin(async move {
                let input: Bump = input.into();
                apply_bump::<$agg>(cx, &input).await
            })
        }
    };
}

bump_input!(BumpCrud, bump_crud, CrudCounter, "bump_crud");
bump_input!(BumpSoft, bump_soft, SoftCounter, "bump_soft");
bump_input!(BumpFull, bump_full, FullCounter, "bump_full");

#[derive(Debug, Deserialize)]
pub struct BumpFullCmd {
    pub key: Uuid,
    pub tenant: Uuid,
    pub amount: i64,
    pub author: String,
}

impl From<BumpFullCmd> for Bump {
    fn from(input: BumpFullCmd) -> Bump {
        Bump {
            key: input.key,
            tenant: input.tenant,
            amount: input.amount,
            author: input.author,
        }
    }
}

pub fn bump_full_coords() -> CommandCoords {
    CommandCoords {
        receiver: Bc::new("sample").unwrap(),
        aggregate: CoordAggregate::new("counter").unwrap(),
        verb: Verb::new("bump").unwrap(),
        version: 1,
    }
}

impl ReactionMessage for BumpFullCmd {
    fn coordinates() -> ReactionCoordinates {
        ReactionCoordinates::Command(bump_full_coords())
    }

    fn decode(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
}

#[derive(Debug)]
pub enum CoarseCounterFault {
    Store(String),
}

impl std::fmt::Display for CoarseCounterFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(detail) => write!(f, "store: {detail}"),
        }
    }
}

impl std::error::Error for CoarseCounterFault {}

impl ReactionError for CoarseCounterFault {
    fn disposition(&self) -> Disposition {
        Disposition::Retry
    }
}

impl From<CounterFault> for CoarseCounterFault {
    fn from(fault: CounterFault) -> Self {
        match fault {
            CounterFault::Refused(reason) => Self::Store(reason.code().to_string()),
            CounterFault::Store(detail) => Self::Store(detail),
        }
    }
}

pub fn bump_full_reaction<'r>(
    cx: &'r mut Reaction<'r>,
    cmd: BumpFullCmd,
) -> BoxFuture<'r, Result<(), CoarseCounterFault>> {
    Box::pin(async move {
        let input: Bump = cmd.into();
        apply_bump::<FullCounter>(cx, &input)
            .await
            .map_err(CoarseCounterFault::from)
    })
}
