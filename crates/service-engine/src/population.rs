use std::collections::BTreeSet;
use std::sync::Arc;

use crate::impact::{Deps, Dims, Impact};
use crate::name::{Namespace, NounName};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Interest {
    pub nouns: BTreeSet<NounName>,
    pub dims: Dims,
    pub foreign: BTreeSet<Namespace>,
    pub deps: Deps,
}

impl Interest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_noun(mut self, noun: NounName, dims: Dims) -> Self {
        self.nouns.insert(noun);
        self.dims = self.dims.union(dims);
        self
    }

    pub fn on_foreign(mut self, namespace: Namespace) -> Self {
        self.foreign.insert(namespace);
        self
    }

    pub fn on_deps(mut self, deps: Deps) -> Self {
        self.deps = self.deps.union(deps);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.nouns.is_empty() && self.foreign.is_empty() && self.deps.is_empty()
    }

    pub fn intersects(&self, impact: &Impact) -> bool {
        match impact {
            Impact::ResourceChanged { noun, dims, .. } => {
                self.nouns.contains(noun)
                    && (self.dims.is_empty() || dims.is_empty() || self.dims.intersects(*dims))
            }
            Impact::ForeignChanged { foreign } => self.foreign.contains(foreign.namespace()),
            Impact::PrincipalFactsChanged { deps, .. } => {
                !self.deps.is_empty() && (deps.is_empty() || self.deps.intersects(*deps))
            }
        }
    }
}

pub type WindowPredicate<K> = Arc<dyn Fn(&K, &Impact) -> bool + Send + Sync>;

#[derive(Clone)]
pub struct WindowQuery<K> {
    interest: Interest,
    predicate: WindowPredicate<K>,
    keys: BTreeSet<K>,
    authoritative: bool,
}

impl<K: Ord> WindowQuery<K> {
    pub fn new(interest: Interest, predicate: WindowPredicate<K>) -> Self {
        Self {
            interest,
            predicate,
            keys: BTreeSet::new(),
            authoritative: false,
        }
    }

    pub fn with_keys(mut self, keys: impl IntoIterator<Item = K>) -> Self {
        self.keys = keys.into_iter().collect();
        self.authoritative = true;
        self
    }

    pub fn interest(&self) -> &Interest {
        &self.interest
    }

    pub fn predicate(&self) -> &WindowPredicate<K> {
        &self.predicate
    }

    pub fn keys(&self) -> &BTreeSet<K> {
        &self.keys
    }

    pub fn authoritative(&self) -> bool {
        self.authoritative
    }
}

impl<K> std::fmt::Debug for WindowQuery<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowQuery")
            .field("interest", &self.interest)
            .field("keys", &self.keys.len())
            .field("authoritative", &self.authoritative)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub enum Population<K> {
    Keys(BTreeSet<K>),
    Ordered { keys: Vec<K>, open_head: bool },
    Query(WindowQuery<K>),
}

#[derive(Debug, Clone)]
pub enum Inverse<K> {
    Keys(BTreeSet<K>),
    Query(WindowQuery<K>),
    None,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::impact::ForeignKey;
    use crate::wire::{KeyBytes, Noun};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
    struct Id(u32);

    struct Assignment;
    impl Noun for Assignment {
        type Key = Id;
        const NAME: NounName = NounName::from_static("assignment");
    }

    fn resource(noun: NounName, dims: Dims) -> Impact {
        Impact::ResourceChanged {
            noun,
            key: KeyBytes::encode(&Id(1)).unwrap(),
            dims,
            cause: None,
        }
    }

    #[test]
    fn an_interest_only_intersects_the_nouns_and_dims_it_declares() {
        let dim = Dims::bit(0).unwrap();
        let other = Dims::bit(1).unwrap();
        let interest = Interest::new().on_noun(Assignment::NAME, dim);
        assert!(interest.intersects(&resource(Assignment::NAME, dim)));
        assert!(!interest.intersects(&resource(Assignment::NAME, other)));
        assert!(!interest.intersects(&resource(NounName::from_static("note"), dim)));
    }

    #[test]
    fn a_dimless_resource_impact_reaches_every_interest_on_its_noun() {
        let interest = Interest::new().on_noun(Assignment::NAME, Dims::bit(0).unwrap());
        assert!(interest.intersects(&resource(Assignment::NAME, Dims::EMPTY)));
    }

    #[test]
    fn an_interest_that_names_a_noun_without_dims_reaches_every_impact_on_that_noun() {
        let interest = Interest::new().on_noun(Assignment::NAME, Dims::EMPTY);
        assert!(interest.intersects(&resource(Assignment::NAME, Dims::bit(0).unwrap())));
        assert!(interest.intersects(&resource(Assignment::NAME, Dims::bit(31).unwrap())));
        assert!(interest.intersects(&resource(Assignment::NAME, Dims::EMPTY)));
        assert!(!interest.intersects(&resource(
            NounName::from_static("note"),
            Dims::bit(0).unwrap()
        )));
    }

    #[test]
    fn an_interest_that_declares_no_deps_is_not_woken_by_principal_facts() {
        let interest = Interest::new().on_noun(Assignment::NAME, Dims::EMPTY);
        assert!(!interest.intersects(&Impact::principal_facts(
            uuid::Uuid::now_v7().into(),
            Deps::EMPTY
        )));
        assert!(!interest.intersects(&Impact::principal_facts(
            uuid::Uuid::now_v7().into(),
            Deps::bit(0).unwrap()
        )));
    }

    #[test]
    fn a_depless_principal_impact_reaches_every_interest_that_declares_deps() {
        let interest = Interest::new().on_deps(Deps::bit(1).unwrap());
        assert!(interest.intersects(&Impact::principal_facts(
            uuid::Uuid::now_v7().into(),
            Deps::EMPTY
        )));
    }

    #[test]
    fn an_interest_intersects_the_foreign_namespaces_and_deps_it_declares() {
        let dep = Deps::bit(2).unwrap();
        let interest = Interest::new()
            .on_foreign(Namespace::new("identity.user").unwrap())
            .on_deps(dep);
        assert!(interest.intersects(&Impact::foreign(
            ForeignKey::new("identity.user", "u1").unwrap()
        )));
        assert!(!interest.intersects(&Impact::foreign(
            ForeignKey::new("identity.group", "g1").unwrap()
        )));
        assert!(interest.intersects(&Impact::principal_facts(uuid::Uuid::now_v7().into(), dep)));
        assert!(!interest.intersects(&Impact::principal_facts(
            uuid::Uuid::now_v7().into(),
            Deps::bit(3).unwrap()
        )));
    }

    #[test]
    fn an_interest_that_declares_nothing_is_empty() {
        assert!(Interest::new().is_empty());
        assert!(!Interest::new().on_deps(Deps::bit(0).unwrap()).is_empty());
    }
}
