use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::nats::KV_PUBLISHED_LANGUAGE;

pub trait Consumed: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
    const PREFIX: &'static str;

    /// Whether an empty authoritative snapshot (including the last key's
    /// retraction) is valid. Required singleton sources keep the default.
    const ALLOW_EMPTY: bool = false;

    fn bucket() -> &'static str {
        KV_PUBLISHED_LANGUAGE
    }
}
