use serde::{Deserialize, Serialize};
use service_engine::name::{NounName, ProjectorName};
use service_engine::nats::KvKey;
use service_engine::presence::Presence;
use service_engine::session::WindowParams;
use service_engine::wire::Noun;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TypingKey {
    pub board: Uuid,
    pub user: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypingValue {
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypingView {
    pub board: Uuid,
    pub user: Uuid,
    pub label: String,
}

pub struct Typing;

impl Typing {
    pub const NAME: ProjectorName = ProjectorName::from_static("typing");
}

impl Noun for Typing {
    type Key = TypingKey;
    const NAME: NounName = NounName::from_static("typing");
}

impl Presence for Typing {
    type Noun = Typing;
    type Value = TypingValue;
    type View = TypingView;

    const NAME: ProjectorName = Self::NAME;

    fn kv_key(key: &TypingKey) -> KvKey {
        KvKey::new(format!(
            "typing/{}/{}",
            key.board.simple(),
            key.user.simple()
        ))
        .expect("a hex-and-slash presence key is always valid")
    }

    fn parse_kv_key(raw: &KvKey) -> Option<TypingKey> {
        let mut parts = raw.as_str().split('/');
        if parts.next()? != "typing" {
            return None;
        }
        let board = Uuid::parse_str(parts.next()?).ok()?;
        let user = Uuid::parse_str(parts.next()?).ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(TypingKey { board, user })
    }

    fn view(key: &TypingKey, value: &TypingValue) -> TypingView {
        TypingView {
            board: key.board,
            user: key.user,
            label: value.label.clone(),
        }
    }

    fn in_window(key: &TypingKey, params: &WindowParams) -> bool {
        params.get::<Uuid>("board") == Some(key.board)
    }
}
