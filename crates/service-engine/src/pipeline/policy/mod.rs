use serde::Serialize;
use sqlx::PgConnection;

use crate::error::EngineError;
use crate::gate::Reason;
use crate::impact::Dims;
use crate::persistence::{Aggregate, Persistence};
use crate::pipeline::ops::Ops;
use crate::pipeline::outbound::{OutboundCommand, OutboundEvent};
use crate::pipeline::staged::RefusalOrigin;
use crate::time::Timestamp;
use crate::wire::Noun;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refused(pub Reason);

impl From<Reason> for Refused {
    fn from(reason: Reason) -> Self {
        Self(reason)
    }
}

pub struct Saved<'a, A: Aggregate> {
    pub prior: Option<&'a A>,
    pub next: &'a A,
    pub events: &'a [<A::Store as Persistence>::Event],
}

impl<A: Aggregate> Saved<'_, A> {
    pub fn transitioned<T, F>(&self, project: F) -> bool
    where
        T: PartialEq,
        F: Fn(&A) -> T,
    {
        match self.prior {
            Some(prior) => project(prior) != project(self.next),
            None => true,
        }
    }
}

pub struct PostSave<'a, 'ops> {
    ops: &'a mut Ops<'ops>,
    origin: RefusalOrigin,
}

impl<'a, 'ops> PostSave<'a, 'ops> {
    pub(crate) fn new(ops: &'a mut Ops<'ops>, origin: RefusalOrigin) -> Self {
        Self { ops, origin }
    }

    pub fn now(&self) -> Timestamp {
        self.ops.now()
    }

    pub fn connection(&mut self) -> &mut PgConnection {
        self.ops.connection()
    }

    pub fn impact<N: Noun>(&mut self, key: &N::Key, dims: Dims) -> Result<(), EngineError> {
        self.ops.impact::<N>(key, dims)
    }

    pub fn impact_caused<N: Noun, C: Serialize>(
        &mut self,
        key: &N::Key,
        cause: C,
    ) -> Result<(), EngineError> {
        self.ops.impact_caused::<N, C>(key, cause)
    }

    pub fn command<C: OutboundCommand>(&mut self, command: C) -> Result<(), EngineError> {
        self.ops.command(command)
    }

    pub fn emit<E: OutboundEvent>(&mut self, event: E) -> Result<(), EngineError> {
        self.ops.emit(event)
    }

    pub fn refuse(&mut self, reason: Reason) -> Refused {
        self.ops.record_policy_refusal(reason, self.origin);
        Refused(reason)
    }
}

mod registry;
pub(crate) use registry::{AggregatePolicies, AggregatePolicy, Policies, PolicySeams};

#[cfg(test)]
mod tests;
