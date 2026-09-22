mod projection;
mod publish;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use service_engine::Consumed;
use service_engine::name::MirrorName;
use service_engine::nats::KvKey;
use uuid::Uuid;

pub use projection::{
    backfills, directory_mirror, directory_mirror_handle, known_users, required_key_mirror,
};
pub use publish::{
    mirror_dead_letters, publish_offer_manifest, publish_required_user, publish_roster,
    publish_versioned_user, read_offer_manifest, retract_required_user, retract_user,
};

pub const DIRECTORY_MIRROR: MirrorName = MirrorName::from_static("directory");
const USER_PREFIX: &str = "identity/users/";
const USER_NAMESPACE: &str = "identity.user";

pub const REQUIRED_USER_KEY: &str = "identity/users/00000000-0000-0000-0000-000000000009";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SamplePublishedUser {
    pub email: String,
    #[serde(default)]
    pub wire: Option<u16>,
}

impl Consumed for SamplePublishedUser {
    const PREFIX: &'static str = USER_PREFIX;

    fn wire_version(value: &Self) -> Option<u16> {
        value.wire
    }
}

pub struct SampleDirectory {
    users: BTreeMap<Uuid, SamplePublishedUser>,
}

impl SampleDirectory {
    pub fn with_users(users: &[(Uuid, &str)]) -> Self {
        Self {
            users: users
                .iter()
                .map(|(id, email)| {
                    (
                        *id,
                        SamplePublishedUser {
                            email: (*email).to_string(),
                            wire: None,
                        },
                    )
                })
                .collect(),
        }
    }
}

fn user_key(id: Uuid) -> KvKey {
    KvKey::new(format!("{USER_PREFIX}{id}")).expect("a published-user key is valid")
}

fn id_of(key: &KvKey) -> Option<Uuid> {
    key.as_str()
        .strip_prefix(USER_PREFIX)
        .and_then(|raw| Uuid::parse_str(raw).ok())
}

pub fn user_prefix() -> &'static str {
    USER_PREFIX
}
