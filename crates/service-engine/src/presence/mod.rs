mod lane;
mod projector;
mod store;
mod watch;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::name::ProjectorName;
use crate::nats::KvKey;
use crate::session::WindowParams;
use crate::wire::Noun;

pub(crate) use lane::REASON_PRESENCE_BUCKET;
pub use lane::{PresenceHandle, PresenceRegistry};
pub(crate) use projector::PresenceProjector;
pub(crate) use watch::{bind_and_seed, presence_channel, run_watch};

pub type PresenceKey<Pr> = <<Pr as Presence>::Noun as Noun>::Key;

pub trait Presence: Send + Sync + 'static {
    type Noun: Noun;
    type Value: Clone + Send + Sync + Serialize + DeserializeOwned + 'static;
    type View: Clone + PartialEq + Serialize + Send + Sync + 'static;

    const NAME: ProjectorName;

    fn kv_key(key: &PresenceKey<Self>) -> KvKey;

    fn parse_kv_key(raw: &KvKey) -> Option<PresenceKey<Self>>;

    fn view(key: &PresenceKey<Self>, value: &Self::Value) -> Self::View;

    fn in_window(key: &PresenceKey<Self>, params: &WindowParams) -> bool;
}
