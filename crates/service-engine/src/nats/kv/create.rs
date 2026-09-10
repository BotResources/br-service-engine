use async_nats::jetstream::kv::CreateErrorKind;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::nats::NatsError;
use crate::nats::key::KvKey;

use super::{KvBucket, Revision, encode, kv_error};

impl<V> KvBucket<V>
where
    V: Serialize + DeserializeOwned,
{
    pub async fn create(&self, key: &KvKey, value: &V) -> Result<Revision, NatsError> {
        let bytes = encode(value)?;
        match self.store().create(key.as_str(), bytes.into()).await {
            Ok(revision) => Ok(Revision::new(revision)),
            Err(error) if error.kind() == CreateErrorKind::AlreadyExists => {
                Err(NatsError::RevisionConflict {
                    key: key.as_str().to_string(),
                    expected: 0,
                })
            }
            Err(error) => Err(kv_error(key, &error)),
        }
    }
}
