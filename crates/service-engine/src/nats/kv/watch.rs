use async_nats::jetstream::kv::Operation;
use serde::de::DeserializeOwned;

use super::{KvBucket, Revision, Slot};
use crate::nats::NatsError;
use crate::nats::key::{KvKey, KvPrefix};

pub struct KvWatch {
    inner: async_nats::jetstream::kv::Watch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvEvent<V> {
    Put {
        key: KvKey,
        value: V,
        revision: Revision,
    },
    Delete {
        key: KvKey,
        revision: Revision,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Watched<V> {
    Event(KvEvent<V>),
    Boundary(u64),
}

impl KvWatch {
    pub async fn next<V: DeserializeOwned>(&mut self) -> Option<Result<KvEvent<V>, NatsError>> {
        use futures_util::StreamExt;
        loop {
            let entry = match self.inner.next().await {
                Some(Ok(entry)) => entry,
                Some(Err(error)) => return Some(Err(watch_error(&error))),
                None => return None,
            };
            if let Some(event) = entry_to_event::<V>(entry) {
                return Some(event);
            }
        }
    }

    pub async fn next_under<V: DeserializeOwned>(
        &mut self,
        prefix: &KvPrefix,
    ) -> Option<Result<Watched<V>, NatsError>> {
        self.next_where(|key| prefix.matches(key)).await
    }

    pub(crate) async fn next_where<V: DeserializeOwned>(
        &mut self,
        matches: impl Fn(&str) -> bool,
    ) -> Option<Result<Watched<V>, NatsError>> {
        Some(
            self.next_seen_where::<V>(matches)
                .await?
                .and_then(|seen| match seen {
                    Seen::Boundary(revision) => Ok(Watched::Boundary(revision)),
                    Seen::Delete { key, revision } => {
                        Ok(Watched::Event(KvEvent::Delete { key, revision }))
                    }
                    Seen::Put {
                        key,
                        slot,
                        revision,
                    } => {
                        let value = slot.strict(&key)?;
                        Ok(Watched::Event(KvEvent::Put {
                            key,
                            value,
                            revision,
                        }))
                    }
                }),
        )
    }

    pub(crate) async fn next_seen_where<V: DeserializeOwned>(
        &mut self,
        matches: impl Fn(&str) -> bool,
    ) -> Option<Result<Seen<V>, NatsError>> {
        use futures_util::StreamExt;
        let entry = match self.inner.next().await {
            Some(Ok(entry)) => entry,
            Some(Err(error)) => return Some(Err(watch_error(&error))),
            None => return None,
        };
        let revision = entry.revision;
        if !matches(&entry.key) {
            return Some(Ok(Seen::Boundary(revision)));
        }
        Some(Ok(
            entry_to_seen::<V>(entry).unwrap_or(Seen::Boundary(revision))
        ))
    }
}

pub(crate) enum Seen<V> {
    Put {
        key: KvKey,
        slot: Slot<V>,
        revision: Revision,
    },
    Delete {
        key: KvKey,
        revision: Revision,
    },
    Boundary(u64),
}

fn entry_to_seen<V: DeserializeOwned>(entry: async_nats::jetstream::kv::Entry) -> Option<Seen<V>> {
    let key = KvKey::new(entry.key.clone()).ok()?;
    let revision = Revision::new(entry.revision);
    Some(match entry.operation {
        Operation::Delete | Operation::Purge => Seen::Delete { key, revision },
        Operation::Put => Seen::Put {
            key,
            slot: Slot::<V>::from_json(&entry.value),
            revision,
        },
    })
}

fn entry_to_event<V: DeserializeOwned>(
    entry: async_nats::jetstream::kv::Entry,
) -> Option<Result<KvEvent<V>, NatsError>> {
    Some(match entry_to_seen::<V>(entry)? {
        Seen::Delete { key, revision } => Ok(KvEvent::Delete { key, revision }),
        Seen::Put {
            key,
            slot,
            revision,
        } => slot.strict(&key).map(|value| KvEvent::Put {
            key,
            value,
            revision,
        }),
        Seen::Boundary(_) => return None,
    })
}

fn watch_error(detail: &dyn std::fmt::Display) -> NatsError {
    NatsError::Kv {
        key: "watch".to_string(),
        detail: detail.to_string(),
    }
}

impl<V> KvBucket<V> {
    pub async fn watch_all(&self) -> Result<KvWatch, NatsError> {
        let inner = self.store().watch_all().await.map_err(|e| NatsError::Kv {
            key: "watch".to_string(),
            detail: crate::chain::describe(&e),
        })?;
        Ok(KvWatch { inner })
    }

    pub async fn watch_all_from(&self, revision: u64) -> Result<KvWatch, NatsError> {
        if revision == 0 {
            return self.watch_all().await;
        }
        let inner = self
            .store()
            .watch_all_from_revision(revision)
            .await
            .map_err(|e| NatsError::Kv {
                key: "watch".to_string(),
                detail: crate::chain::describe(&e),
            })?;
        Ok(KvWatch { inner })
    }
}
