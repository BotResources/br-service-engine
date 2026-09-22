use std::sync::Arc;

use crate::nats::KvKey;

use super::consumed::Consumed;
use super::shadow::Shadows;

type Presence = Arc<dyn Fn(&Shadows) -> bool + Send + Sync>;

pub(super) struct RequiredKey {
    prefix: &'static str,
    key: &'static str,
    present: Presence,
}

impl RequiredKey {
    pub(super) fn of<C: Consumed>(key: &'static str) -> Self {
        Self {
            prefix: C::PREFIX,
            key,
            present: Arc::new(move |shadows: &Shadows| {
                KvKey::new(key)
                    .ok()
                    .is_some_and(|kv| shadows.shadow::<C>().get(&kv).is_some())
            }),
        }
    }

    pub(super) fn prefix(&self) -> &'static str {
        self.prefix
    }

    pub(super) fn key(&self) -> &'static str {
        self.key
    }

    pub(super) fn within_prefix(&self) -> bool {
        self.key.starts_with(self.prefix)
    }

    pub(super) fn is_present(&self, shadows: &Shadows) -> bool {
        (self.present)(shadows)
    }
}
