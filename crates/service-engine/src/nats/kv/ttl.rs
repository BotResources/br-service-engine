use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::{KvBucket, encode, kv_error};
use crate::nats::NatsError;
use crate::nats::key::KvKey;

impl<V> KvBucket<V>
where
    V: Serialize + DeserializeOwned,
{
    pub async fn put_with_ttl(
        &self,
        key: &KvKey,
        value: &V,
        ttl: Duration,
    ) -> Result<(), NatsError> {
        let store = self.store();
        if store.use_jetstream_prefix {
            return Err(NatsError::Kv {
                key: key.as_str().to_string(),
                detail: "a jetstream domain prefix is configured, which the per-key TTL put path \
                         does not build a subject for"
                    .to_string(),
            });
        }
        let subject = format!(
            "{}{}",
            store.put_prefix.as_deref().unwrap_or(&store.prefix),
            key.as_str()
        );
        let bytes = encode(value)?;
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(
            async_nats::header::NATS_MESSAGE_TTL,
            async_nats::HeaderValue::from(ttl.as_secs()),
        );
        let ack = self
            .context()
            .publish_with_headers(subject, headers, bytes.into())
            .await
            .map_err(|e| kv_error(key, &e))?;
        ack.await.map_err(|e| kv_error(key, &e))?;
        Ok(())
    }
}
