use br_core_auth::{AuthMethod, Passport, PassportClaims};
use futures_util::future::BoxFuture;
use service_engine::{PassportPrincipal, PrincipalRejected};
use sqlx::PgPool;
use uuid::Uuid;

use crate::sample::principal::SamplePrincipal;

pub const TENANT_CLAIM: &str = "tenant";

pub fn passport_for(user: Uuid, tenant: Uuid) -> Passport {
    let mut map = serde_json::Map::new();
    map.insert(
        TENANT_CLAIM.to_string(),
        serde_json::Value::String(tenant.to_string()),
    );
    let claims = PassportClaims::from_map(map);
    Passport::human(user, false, true, AuthMethod::Jwt, None, claims)
}

impl PassportPrincipal for SamplePrincipal {
    fn from_passport(
        _pg: &PgPool,
        passport: Passport,
    ) -> BoxFuture<'_, Result<Self, PrincipalRejected>> {
        Box::pin(async move {
            let user = passport
                .user_id()
                .ok_or_else(|| PrincipalRejected::new("a human passport is required"))?;
            let tenant = passport
                .claim::<Uuid>(TENANT_CLAIM)
                .ok_or_else(|| PrincipalRejected::new("the passport carries no tenant claim"))?;
            Ok(SamplePrincipal::new(user, tenant))
        })
    }
}
