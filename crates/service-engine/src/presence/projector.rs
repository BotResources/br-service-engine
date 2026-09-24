use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::cohort::{Cohort, CohortKey};
use crate::error::EngineError;
use crate::impact::{Dims, ForeignKey};
use crate::name::{NounName, ProjectorName};
use crate::page::KeyCeiling;
use crate::population::{Interest, Inverse, Population, WindowPredicate, WindowQuery};
use crate::presence::store::PresenceStore;
use crate::presence::{Presence, PresenceKey};
use crate::principal::Principal;
use crate::projector::{LoadScope, Projector};
use crate::session::WindowParams;
use crate::wire::Noun;

pub(crate) struct PresenceProjector<P, Pr: Presence> {
    store: Arc<PresenceStore<PresenceKey<Pr>, Pr::Value>>,
    _principal: PhantomData<fn() -> P>,
    _presence: PhantomData<fn() -> Pr>,
}

impl<P, Pr: Presence> PresenceProjector<P, Pr> {
    pub(crate) fn new(store: Arc<PresenceStore<PresenceKey<Pr>, Pr::Value>>) -> Self {
        Self {
            store,
            _principal: PhantomData,
            _presence: PhantomData,
        }
    }
}

impl<P: Principal, Pr: Presence> Projector for PresenceProjector<P, Pr> {
    type Principal = P;
    type Key = PresenceKey<Pr>;
    type Facts = BTreeMap<PresenceKey<Pr>, Pr::Value>;
    type View = Pr::View;

    fn name(&self) -> ProjectorName {
        Pr::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        struct Holder<Pr>(PhantomData<Pr>);
        impl<Pr: Presence> Holder<Pr> {
            const NOUNS: &'static [NounName] = &[<Pr::Noun as Noun>::NAME];
        }
        Holder::<Pr>::NOUNS
    }

    fn populate<'a>(
        &'a self,
        _pg: &'a sqlx::PgPool,
        window: &'a WindowParams,
        ceiling: KeyCeiling,
        _principal: &'a P,
    ) -> BoxFuture<'a, Result<Population<Self::Key>, EngineError>> {
        Box::pin(async move {
            let members: Vec<Self::Key> = self
                .store
                .keys()
                .into_iter()
                .filter(|key| Pr::in_window(key, window))
                .take(ceiling.get())
                .collect();
            let interest = Interest::new().on_noun(<Pr::Noun as Noun>::NAME, Dims::EMPTY);
            let predicate: WindowPredicate<Self::Key> = Arc::new(|_key, _impact| false);
            Ok(Population::Query(
                WindowQuery::new(interest, predicate).with_keys(members),
            ))
        })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<Self::Key> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Self::Key, P>,
    ) -> BoxFuture<'a, Result<Self::Facts, EngineError>> {
        Box::pin(async move { Ok(self.store.get_many(scope.keys())) })
    }

    fn project(
        &self,
        facts: &Self::Facts,
        key: &Self::Key,
        _principal: &P,
    ) -> Result<Option<Self::View>, EngineError> {
        Ok(facts.get(key).map(|value| Pr::view(key, value)))
    }

    fn cohort(&self, _principal: &P) -> CohortKey {
        Cohort::text("presence", Pr::NAME.as_str()).key()
    }
}
