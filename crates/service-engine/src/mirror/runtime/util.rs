use std::collections::HashSet;
use std::hash::Hash;

use super::Revisions;

pub(super) fn wire_version_reason(expected: u16, found: u16) -> String {
    format!(
        "mirror value carries wire version {found}, but this consumer is coded against version \
         {expected}; a breaking change moves the version into a new prefix"
    )
}

pub(super) fn merge_forward(into: &mut Revisions, from: &Revisions) {
    for (bucket, snapshot) in from {
        match into.get_mut(bucket) {
            Some(held) if held.created == snapshot.created => {
                held.revision = held.revision.max(snapshot.revision);
            }
            _ => {
                into.insert(bucket.clone(), snapshot.clone());
            }
        }
    }
}

pub(super) fn dedup<K: Clone + Eq + Hash>(keys: Vec<K>) -> Vec<K> {
    let mut seen = HashSet::new();
    keys.into_iter()
        .filter(|key| seen.insert(key.clone()))
        .collect()
}
