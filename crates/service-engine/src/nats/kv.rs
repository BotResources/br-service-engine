use std::marker::PhantomData;

use async_nats::jetstream::kv::{Operation, Store};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::nats::NatsError;
use crate::nats::key::{KvKey, KvPrefix};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Revision(u64);

impl Revision {
    pub fn get(&self) -> u64 {
        self.0
    }
}

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
                Some(Err(error)) => {
                    return Some(Err(NatsError::Kv {
                        key: "watch".to_string(),
                        detail: error.to_string(),
                    }));
                }
                None => return None,
            };
            let key = match KvKey::new(entry.key.clone()) {
                Ok(key) => key,
                Err(_) => continue,
            };
            let revision = Revision(entry.revision);
            return Some(match entry.operation {
                Operation::Delete | Operation::Purge => Ok(KvEvent::Delete { key, revision }),
                Operation::Put => decode::<V>(&key, &entry.value).map(|value| KvEvent::Put {
                    key,
                    value,
                    revision,
                }),
            });
        }
    }
}

pub struct KvBucket<V> {
    store: Store,
    _value: PhantomData<V>,
}

impl<V> Clone for KvBucket<V> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            _value: PhantomData,
        }
    }
}

impl<V> KvBucket<V> {
    pub(crate) fn bind(store: Store) -> Self {
        Self {
            store,
            _value: PhantomData,
        }
    }

    pub async fn max_age(&self) -> Result<std::time::Duration, NatsError> {
        self.store
            .status()
            .await
            .map(|status| status.max_age())
            .map_err(|error| NatsError::Kv {
                key: "status".to_string(),
                detail: error.to_string(),
            })
    }

    pub async fn retract(&self, key: &KvKey) -> Result<(), NatsError> {
        self.store
            .delete(key.as_str())
            .await
            .map_err(|e| kv_error(key, &e))?;
        Ok(())
    }

    pub async fn watch_all(&self) -> Result<KvWatch, NatsError> {
        let inner = self.store.watch_all().await.map_err(|e| NatsError::Kv {
            key: "watch".to_string(),
            detail: e.to_string(),
        })?;
        Ok(KvWatch { inner })
    }
}

impl<V> KvBucket<V>
where
    V: Serialize + DeserializeOwned,
{
    pub async fn put(&self, key: &KvKey, value: &V) -> Result<(), NatsError> {
        let bytes = encode(value)?;
        self.store
            .put(key.as_str(), bytes.into())
            .await
            .map_err(|e| kv_error(key, &e))?;
        Ok(())
    }

    pub async fn get(&self, key: &KvKey) -> Result<Option<V>, NatsError> {
        Ok(self.get_with_revision(key).await?.map(|(value, _)| value))
    }

    pub async fn get_with_revision(&self, key: &KvKey) -> Result<Option<(V, Revision)>, NatsError> {
        let Some(entry) = self
            .store
            .entry(key.as_str())
            .await
            .map_err(|e| kv_error(key, &e))?
        else {
            return Ok(None);
        };
        if matches!(entry.operation, Operation::Delete | Operation::Purge) {
            return Ok(None);
        }
        let value = decode::<V>(key, &entry.value)?;
        Ok(Some((value, Revision(entry.revision))))
    }

    pub async fn update_if(
        &self,
        key: &KvKey,
        value: &V,
        expected: Revision,
    ) -> Result<Revision, NatsError> {
        let bytes = encode(value)?;
        match self
            .store
            .update(key.as_str(), bytes.into(), expected.0)
            .await
        {
            Ok(revision) => Ok(Revision(revision)),
            Err(error) => Err(revision_error(key, expected, error.kind(), &error)),
        }
    }

    pub async fn delete_if(&self, key: &KvKey, expected: Revision) -> Result<(), NatsError> {
        match self
            .store
            .delete_expect_revision(key.as_str(), Some(expected.0))
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => Err(revision_error(key, expected, error.kind(), &error)),
        }
    }

    pub async fn entries(&self, prefix: &KvPrefix) -> Result<Vec<(KvKey, V)>, NatsError> {
        use futures_util::StreamExt;
        let mut keys = self
            .store
            .keys()
            .await
            .map_err(|e| NatsError::Kv {
                key: prefix.as_str().to_string(),
                detail: e.to_string(),
            })?
            .boxed();
        let mut out = Vec::new();
        while let Some(next) = keys.next().await {
            let raw = next.map_err(|e| NatsError::Kv {
                key: prefix.as_str().to_string(),
                detail: e.to_string(),
            })?;
            if !prefix.matches(&raw) {
                continue;
            }
            let Ok(key) = KvKey::new(raw) else { continue };
            if let Some((value, _)) = self.get_with_revision(&key).await? {
                out.push((key, value));
            }
        }
        Ok(out)
    }

    pub async fn all(&self) -> Result<Vec<(KvKey, V)>, NatsError> {
        use futures_util::StreamExt;
        let mut keys = self
            .store
            .keys()
            .await
            .map_err(|e| NatsError::Kv {
                key: "keys".to_string(),
                detail: e.to_string(),
            })?
            .boxed();
        let mut out = Vec::new();
        while let Some(next) = keys.next().await {
            let raw = next.map_err(|e| NatsError::Kv {
                key: "keys".to_string(),
                detail: e.to_string(),
            })?;
            let Ok(key) = KvKey::new(raw) else { continue };
            if let Some((value, _)) = self.get_with_revision(&key).await? {
                out.push((key, value));
            }
        }
        Ok(out)
    }
}

fn encode<V: Serialize>(value: &V) -> Result<Vec<u8>, NatsError> {
    serde_json::to_vec(value).map_err(NatsError::Encode)
}

fn decode<V: DeserializeOwned>(key: &KvKey, bytes: &[u8]) -> Result<V, NatsError> {
    serde_json::from_slice(bytes).map_err(|source| NatsError::Decode {
        key: key.as_str().to_string(),
        source,
    })
}

fn kv_error(key: &KvKey, detail: &dyn std::fmt::Display) -> NatsError {
    NatsError::Kv {
        key: key.as_str().to_string(),
        detail: detail.to_string(),
    }
}

fn revision_error(
    key: &KvKey,
    expected: Revision,
    kind: async_nats::jetstream::kv::UpdateErrorKind,
    detail: &dyn std::fmt::Display,
) -> NatsError {
    use async_nats::jetstream::kv::UpdateErrorKind as K;
    match kind {
        K::WrongLastRevision => NatsError::RevisionConflict {
            key: key.as_str().to_string(),
            expected: expected.0,
        },
        _ => kv_error(key, detail),
    }
}
