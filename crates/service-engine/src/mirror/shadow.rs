use std::any::{Any, TypeId};
use std::collections::HashMap;

use crate::nats::KvKey;

use super::consumed::Consumed;

#[derive(Default)]
pub struct Shadows {
    maps: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl Shadows {
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies one revision of a key. The scan and the watch overlap by design —
    /// the watch resumes at the boundary the scan reached — so a shadow carries
    /// the revision it holds and refuses anything older: whichever of the two
    /// arrives last, the newer revision is the one that stays.
    pub fn put<C: Consumed>(&mut self, key: KvKey, value: C, revision: u64) {
        let map = self.map_mut::<C>();
        if map.get(&key).is_some_and(|held| held.revision > revision) {
            return;
        }
        map.insert(key, Held { value, revision });
    }

    /// A retract is a revision like any other, and is refused the same way when
    /// the shadow already holds a newer one.
    pub fn remove<C: Consumed>(&mut self, key: &KvKey, revision: u64) {
        let map = self.map_mut::<C>();
        if map.get(key).is_some_and(|held| held.revision > revision) {
            return;
        }
        map.remove(key);
    }

    pub fn shadow<C: Consumed>(&self) -> Shadow<'_, C> {
        Shadow {
            map: self.map_ref::<C>(),
        }
    }

    fn map_ref<C: Consumed>(&self) -> Option<&HashMap<KvKey, Held<C>>> {
        self.maps
            .get(&TypeId::of::<C>())
            .and_then(|held| held.downcast_ref())
    }

    fn map_mut<C: Consumed>(&mut self) -> &mut HashMap<KvKey, Held<C>> {
        self.maps
            .entry(TypeId::of::<C>())
            .or_insert_with(|| Box::new(HashMap::<KvKey, Held<C>>::new()))
            .downcast_mut()
            .expect("a shadow map keeps the type it was created under")
    }
}

/// One shadowed value and the KV revision it came from.
struct Held<C> {
    value: C,
    revision: u64,
}

impl std::fmt::Debug for Shadows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shadows")
            .field("consumptions", &self.maps.len())
            .finish()
    }
}

pub struct Shadow<'a, C> {
    map: Option<&'a HashMap<KvKey, Held<C>>>,
}

impl<'a, C: Consumed> Shadow<'a, C> {
    pub fn get(&self, key: &KvKey) -> Option<&'a C> {
        self.map
            .and_then(|map| map.get(key))
            .map(|held| &held.value)
    }

    pub fn len(&self) -> usize {
        self.map.map_or(0, HashMap::len)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = (&'a KvKey, &'a C)> {
        self.map
            .into_iter()
            .flat_map(HashMap::iter)
            .map(|(key, held)| (key, &held.value))
    }

    pub fn values(&self) -> impl Iterator<Item = &'a C> {
        self.map
            .into_iter()
            .flat_map(HashMap::values)
            .map(|held| &held.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct A(u32);
    impl Consumed for A {
        const PREFIX: &'static str = "a/";
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct B(u32);
    impl Consumed for B {
        const PREFIX: &'static str = "b/";
    }

    fn key(raw: &str) -> KvKey {
        KvKey::new(raw).unwrap()
    }

    #[test]
    fn two_consumed_types_keep_separate_typed_shadows() {
        let mut shadows = Shadows::new();
        shadows.put::<A>(key("a/1"), A(1), 1);
        shadows.put::<B>(key("b/1"), B(9), 1);
        assert_eq!(shadows.shadow::<A>().get(&key("a/1")), Some(&A(1)));
        assert_eq!(shadows.shadow::<B>().get(&key("b/1")), Some(&B(9)));
        assert_eq!(shadows.shadow::<A>().get(&key("b/1")), None);
    }

    #[test]
    fn a_removed_key_leaves_the_shadow_and_an_absent_type_reads_empty() {
        let mut shadows = Shadows::new();
        shadows.put::<A>(key("a/1"), A(1), 1);
        shadows.remove::<A>(&key("a/1"), 2);
        assert!(shadows.shadow::<A>().is_empty());
        assert_eq!(shadows.shadow::<B>().len(), 0);
    }

    #[test]
    fn an_older_revision_never_lands_on_top_of_a_newer_one() {
        let mut shadows = Shadows::new();
        shadows.put::<A>(key("a/1"), A(7), 7);
        shadows.put::<A>(key("a/1"), A(3), 3);
        assert_eq!(
            shadows.shadow::<A>().get(&key("a/1")),
            Some(&A(7)),
            "the scan and the watch overlap, and the newer revision stays"
        );
        shadows.remove::<A>(&key("a/1"), 3);
        assert_eq!(
            shadows.shadow::<A>().get(&key("a/1")),
            Some(&A(7)),
            "a retract older than the value held is refused the same way"
        );
        shadows.put::<A>(key("a/1"), A(8), 8);
        assert_eq!(shadows.shadow::<A>().get(&key("a/1")), Some(&A(8)));
    }
}
