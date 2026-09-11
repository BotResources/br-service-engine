use serde::{Deserialize, Serialize};
use service_engine::name::{NounName, ProjectorName};
use service_engine::nats::KvKey;
use service_engine::presence::Presence;
use service_engine::session::{WindowParams, WindowSpec};
use service_engine::wire::Noun;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TypingKey {
    pub room: Uuid,
    pub user: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypingValue {
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypingView {
    pub room: Uuid,
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
            key.room.simple(),
            key.user.simple()
        ))
        .expect("a hex-and-slash presence key is always valid")
    }

    fn parse_kv_key(raw: &KvKey) -> Option<TypingKey> {
        let mut parts = raw.as_str().split('/');
        if parts.next()? != "typing" {
            return None;
        }
        let room = Uuid::parse_str(parts.next()?).ok()?;
        let user = Uuid::parse_str(parts.next()?).ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(TypingKey { room, user })
    }

    fn view(key: &TypingKey, value: &TypingValue) -> TypingView {
        TypingView {
            room: key.room,
            user: key.user,
            label: value.label.clone(),
        }
    }

    fn in_window(key: &TypingKey, params: &WindowParams) -> bool {
        params.get::<Uuid>("room") == Some(key.room)
    }
}

pub fn typing_key(room: Uuid, user: Uuid) -> TypingKey {
    TypingKey { room, user }
}

pub fn typing_value(label: &str) -> TypingValue {
    TypingValue {
        label: label.to_string(),
    }
}

pub fn typing_window(room: Uuid) -> WindowSpec {
    let params = WindowParams::encode(&RoomWindow { room }).expect("a room window encodes");
    WindowSpec::new(Typing::NAME, params, false)
}

#[derive(Serialize)]
struct RoomWindow {
    room: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CursorKey {
    pub room: Uuid,
    pub user: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorValue {
    pub at: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorView {
    pub room: Uuid,
    pub user: Uuid,
    pub at: u32,
}

pub struct Cursor;

impl Cursor {
    pub const NAME: ProjectorName = ProjectorName::from_static("cursor");
}

impl Noun for Cursor {
    type Key = CursorKey;
    const NAME: NounName = NounName::from_static("cursor");
}

impl Presence for Cursor {
    type Noun = Cursor;
    type Value = CursorValue;
    type View = CursorView;

    const NAME: ProjectorName = Self::NAME;

    fn kv_key(key: &CursorKey) -> KvKey {
        KvKey::new(format!(
            "cursor/{}/{}",
            key.room.simple(),
            key.user.simple()
        ))
        .expect("a hex-and-slash presence key is always valid")
    }

    fn parse_kv_key(raw: &KvKey) -> Option<CursorKey> {
        let mut parts = raw.as_str().split('/');
        if parts.next()? != "cursor" {
            return None;
        }
        let room = Uuid::parse_str(parts.next()?).ok()?;
        let user = Uuid::parse_str(parts.next()?).ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(CursorKey { room, user })
    }

    fn view(key: &CursorKey, value: &CursorValue) -> CursorView {
        CursorView {
            room: key.room,
            user: key.user,
            at: value.at,
        }
    }

    fn in_window(key: &CursorKey, params: &WindowParams) -> bool {
        params.get::<Uuid>("room") == Some(key.room)
    }
}

pub fn cursor_key(room: Uuid, user: Uuid) -> CursorKey {
    CursorKey { room, user }
}

pub fn cursor_value(at: u32) -> CursorValue {
    CursorValue { at }
}

pub fn cursor_window(room: Uuid) -> WindowSpec {
    let params = WindowParams::encode(&RoomWindow { room }).expect("a room window encodes");
    WindowSpec::new(Cursor::NAME, params, false)
}
