use async_nats::jetstream::kv::Operation;
use serde::de::DeserializeOwned;

use super::{KvBucket, Revision, decode};
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
    ) -> Option<Result<KvEvent<V>, NatsError>> {
        use futures_util::StreamExt;
        loop {
            let entry = match self.inner.next().await {
                Some(Ok(entry)) => entry,
                Some(Err(error)) => return Some(Err(watch_error(&error))),
                None => return None,
            };
            if !prefix.matches(&entry.key) {
                continue;
            }
            if let Some(event) = entry_to_event::<V>(entry) {
                return Some(event);
            }
        }
    }
}

fn entry_to_event<V: DeserializeOwned>(
    entry: async_nats::jetstream::kv::Entry,
) -> Option<Result<KvEvent<V>, NatsError>> {
    let key = KvKey::new(entry.key.clone()).ok()?;
    let revision = Revision::new(entry.revision);
    Some(match entry.operation {
        Operation::Delete | Operation::Purge => Ok(KvEvent::Delete { key, revision }),
        Operation::Put => decode::<V>(&key, &entry.value).map(|value| KvEvent::Put {
            key,
            value,
            revision,
        }),
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
            detail: e.to_string(),
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
                detail: e.to_string(),
            })?;
        Ok(KvWatch { inner })
    }
}
