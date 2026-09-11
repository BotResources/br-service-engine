use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::nats::KV_PUBLISHED_LANGUAGE;

pub trait Consumed: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
    const PREFIX: &'static str;

    fn bucket() -> &'static str {
        KV_PUBLISHED_LANGUAGE
    }
}
