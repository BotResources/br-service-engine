use std::ops::{Deref, DerefMut};

use futures_util::future::BoxFuture;

use crate::error::EngineError;
use crate::impact::Impact;
use crate::pipeline::ops::Ops;
use crate::presence::{Presence, PresenceHandle, PresenceKey};
use crate::principal::Principal;
use crate::projector::Projector;

pub(crate) type PresencePut =
    Box<dyn FnOnce() -> BoxFuture<'static, Result<(), EngineError>> + Send>;

pub struct Reaction<'a> {
    ops: Ops<'a>,
}

impl<'a> Reaction<'a> {
    pub(crate) fn new(ops: Ops<'a>) -> Self {
        Self { ops }
    }
}

impl<'a> Deref for Reaction<'a> {
    type Target = Ops<'a>;

    fn deref(&self) -> &Ops<'a> {
        &self.ops
    }
}

impl DerefMut for Reaction<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ops
    }
}

pub struct Mutation<'a, P: Principal> {
    ops: Ops<'a>,
    principal: &'a P,
    presence: &'a PresenceHandle<P>,
    presence_puts: &'a mut Vec<PresencePut>,
}

impl<'a, P: Principal> Mutation<'a, P> {
    pub(crate) fn new(
        ops: Ops<'a>,
        principal: &'a P,
        presence: &'a PresenceHandle<P>,
        presence_puts: &'a mut Vec<PresencePut>,
    ) -> Self {
        Self {
            ops,
            principal,
            presence,
            presence_puts,
        }
    }

    pub fn principal(&self) -> &P {
        self.principal
    }

    pub fn present<Pr: Presence>(&mut self, key: &PresenceKey<Pr>, value: &Pr::Value) {
        let handle = self.presence.clone();
        let key = key.clone();
        let value = value.clone();
        self.presence_puts.push(Box::new(move || {
            Box::pin(async move { handle.present::<Pr>(&key, &value).await })
        }));
    }
}

impl<'a, P: Principal> Deref for Mutation<'a, P> {
    type Target = Ops<'a>;

    fn deref(&self) -> &Ops<'a> {
        &self.ops
    }
}

impl<P: Principal> DerefMut for Mutation<'_, P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ops
    }
}

pub struct Bulk<'a, P: Principal> {
    ops: Ops<'a>,
    principal: &'a P,
}

impl<'a, P: Principal> Bulk<'a, P> {
    pub(crate) fn new(ops: Ops<'a>, principal: &'a P) -> Self {
        Self { ops, principal }
    }

    pub fn principal(&self) -> &P {
        self.principal
    }

    pub fn impact_all<Pr: Projector<Principal = P>>(&mut self, projector: Pr) {
        self.ops
            .staged
            .impacts
            .push(Impact::projector_reset(projector.name()));
    }

    pub fn impact_all_view<V: crate::view::Projector<Principal = P>>(&mut self) {
        self.ops
            .staged
            .impacts
            .push(Impact::projector_reset(V::NAME));
    }
}

impl<'a, P: Principal> Deref for Bulk<'a, P> {
    type Target = Ops<'a>;

    fn deref(&self) -> &Ops<'a> {
        &self.ops
    }
}

impl<P: Principal> DerefMut for Bulk<'_, P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ops
    }
}
