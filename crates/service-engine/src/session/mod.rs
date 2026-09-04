pub(crate) mod live;
pub(crate) mod store;
pub mod stream;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::EngineError;
use crate::name::ProjectorName;
use crate::principal::Principal;

pub use stream::SessionStream;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(Uuid);

impl SessionId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct WindowParams(serde_json::Value);

impl WindowParams {
    pub fn none() -> Self {
        Self(serde_json::Value::Null)
    }

    pub fn encode<T: Serialize>(value: &T) -> Result<Self, EngineError> {
        serde_json::to_value(value)
            .map(Self)
            .map_err(|source| EngineError::Encode {
                what: "window params",
                source,
            })
    }

    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, EngineError> {
        serde_json::from_value(self.0.clone()).map_err(|source| EngineError::Decode {
            what: "window params",
            source,
        })
    }

    pub fn get<T: DeserializeOwned>(&self, field: &str) -> Option<T> {
        self.0
            .get(field)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    pub fn as_value(&self) -> &serde_json::Value {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowSpec {
    pub projector: ProjectorName,
    pub params: WindowParams,
    pub rls: bool,
}

impl WindowSpec {
    pub fn new(projector: ProjectorName, params: WindowParams, rls: bool) -> Self {
        Self {
            projector,
            params,
            rls,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AttachRequest<P: Principal> {
    pub principal: P,
    pub windows: Vec<WindowSpec>,
}

impl<P: Principal> AttachRequest<P> {
    pub fn new(principal: P, windows: Vec<WindowSpec>) -> Self {
        Self { principal, windows }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Inbox {
        limit: u32,
    }

    #[test]
    fn two_sessions_never_share_an_identifier() {
        assert_ne!(SessionId::new(), SessionId::new());
    }

    #[test]
    fn window_params_carry_the_services_own_shape_opaquely() {
        let params = WindowParams::encode(&Inbox { limit: 50 }).unwrap();
        assert_eq!(params.decode::<Inbox>().unwrap(), Inbox { limit: 50 });
        assert_eq!(params.get::<u32>("limit"), Some(50));
        assert_eq!(params.get::<u32>("absent"), None);
    }

    #[test]
    fn window_params_decoding_into_the_wrong_shape_fails_typed() {
        let params = WindowParams::encode(&Inbox { limit: 50 }).unwrap();
        assert!(matches!(
            params.decode::<u32>(),
            Err(EngineError::Decode {
                what: "window params",
                ..
            })
        ));
    }
}
