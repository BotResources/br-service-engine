use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

pub(crate) struct PresenceStore<K, V> {
    values: Mutex<BTreeMap<K, V>>,
}

impl<K: Ord + Clone, V: Clone> PresenceStore<K, V> {
    pub(crate) fn new() -> Self {
        Self {
            values: Mutex::new(BTreeMap::new()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<K, V>> {
        self.values.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(crate) fn put(&self, key: K, value: V) {
        self.lock().insert(key, value);
    }

    pub(crate) fn remove(&self, key: &K) -> bool {
        self.lock().remove(key).is_some()
    }

    pub(crate) fn reseed(&self, entries: impl IntoIterator<Item = (K, V)>) {
        let mut held = self.lock();
        held.clear();
        held.extend(entries);
    }

    pub(crate) fn keys(&self) -> BTreeSet<K> {
        self.lock().keys().cloned().collect()
    }

    pub(crate) fn get_many(&self, keys: &[K]) -> BTreeMap<K, V> {
        let held = self.lock();
        keys.iter()
            .filter_map(|key| held.get(key).map(|value| (key.clone(), value.clone())))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_write_wins_and_a_removed_key_is_gone() {
        let store: PresenceStore<String, u32> = PresenceStore::new();
        store.put("a".into(), 1);
        store.put("a".into(), 2);
        assert_eq!(store.get_many(&["a".to_string()]).get("a"), Some(&2));
        assert!(store.remove(&"a".to_string()));
        assert!(!store.remove(&"a".to_string()));
        assert!(store.get_many(&["a".to_string()]).is_empty());
    }

    #[test]
    fn reseed_replaces_the_whole_snapshot() {
        let store: PresenceStore<String, u32> = PresenceStore::new();
        store.put("stale".into(), 9);
        store.reseed([("x".to_string(), 1), ("y".to_string(), 2)]);
        assert_eq!(
            store.keys(),
            ["x".to_string(), "y".to_string()].into_iter().collect()
        );
    }

    #[test]
    fn get_many_returns_only_present_keys() {
        let store: PresenceStore<String, u32> = PresenceStore::new();
        store.put("here".into(), 7);
        let facts = store.get_many(&["here".to_string(), "absent".to_string()]);
        assert_eq!(facts.len(), 1);
        assert_eq!(facts.get("here"), Some(&7));
    }
}
