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

    pub fn put<C: Consumed>(&mut self, key: KvKey, value: C) {
        self.map_mut::<C>().insert(key, value);
    }

    pub fn remove<C: Consumed>(&mut self, key: &KvKey) {
        self.map_mut::<C>().remove(key);
    }

    pub fn shadow<C: Consumed>(&self) -> Shadow<'_, C> {
        Shadow {
            map: self.map_ref::<C>(),
        }
    }

    fn map_ref<C: Consumed>(&self) -> Option<&HashMap<KvKey, C>> {
        self.maps
            .get(&TypeId::of::<C>())
            .and_then(|held| held.downcast_ref())
    }

    fn map_mut<C: Consumed>(&mut self) -> &mut HashMap<KvKey, C> {
        self.maps
            .entry(TypeId::of::<C>())
            .or_insert_with(|| Box::new(HashMap::<KvKey, C>::new()))
            .downcast_mut()
            .expect("a shadow map keeps the type it was created under")
    }
}

impl std::fmt::Debug for Shadows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shadows")
            .field("consumptions", &self.maps.len())
            .finish()
    }
}

pub struct Shadow<'a, C> {
    map: Option<&'a HashMap<KvKey, C>>,
}

impl<'a, C: Consumed> Shadow<'a, C> {
    pub fn get(&self, key: &KvKey) -> Option<&'a C> {
        self.map.and_then(|map| map.get(key))
    }

    pub fn len(&self) -> usize {
        self.map.map_or(0, HashMap::len)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = (&'a KvKey, &'a C)> {
        self.map.into_iter().flat_map(HashMap::iter)
    }

    pub fn values(&self) -> impl Iterator<Item = &'a C> {
        self.map.into_iter().flat_map(HashMap::values)
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
        shadows.put::<A>(key("a/1"), A(1));
        shadows.put::<B>(key("b/1"), B(9));
        assert_eq!(shadows.shadow::<A>().get(&key("a/1")), Some(&A(1)));
        assert_eq!(shadows.shadow::<B>().get(&key("b/1")), Some(&B(9)));
        assert_eq!(shadows.shadow::<A>().get(&key("b/1")), None);
    }

    #[test]
    fn a_removed_key_leaves_the_shadow_and_an_absent_type_reads_empty() {
        let mut shadows = Shadows::new();
        shadows.put::<A>(key("a/1"), A(1));
        shadows.remove::<A>(&key("a/1"));
        assert!(shadows.shadow::<A>().is_empty());
        assert_eq!(shadows.shadow::<B>().len(), 0);
    }
}
