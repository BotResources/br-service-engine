use br_core_auth::Passport;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::error::EngineError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PrincipalId(Uuid);

impl PrincipalId {
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl From<Uuid> for PrincipalId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for PrincipalId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

pub trait Principal: Clone + Send + Sync + 'static {
    fn id(&self) -> PrincipalId;
    fn passport(&self) -> &Passport;
}

pub trait RlsApplier<P: Principal>: Send + Sync + 'static {
    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        principal: &'a P,
    ) -> BoxFuture<'a, Result<(), EngineError>>;
}

pub trait PrincipalResolver<P: Principal>: Send + Sync + 'static {
    fn resolve<'a>(
        &'a self,
        pg: &'a PgPool,
        current: &'a P,
    ) -> BoxFuture<'a, Result<Option<P>, EngineError>>;
}
