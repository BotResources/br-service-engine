use std::any::Any;

use br_core_auth::{Passport, PassportClaims};
use br_core_integration::Actor;
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::principal::{Principal, PrincipalId, PrincipalResolver, RlsApplier};
use service_engine::{PassportPrincipal, PrincipalRejected};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::kernel::facts::PrincipalFacts;

pub const ORG_CLAIM: &str = "org";
pub const USER_SESSION_VAR: &str = "app.current_user_id";
pub const ORG_SESSION_VAR: &str = "app.current_org_id";

pub const SCOPES_CLAIM: &str = "scopes";

#[derive(Debug, Clone)]
pub struct AppPrincipal {
    id: PrincipalId,
    org: Uuid,
    scopes: Vec<String>,
    super_admin: bool,
    passport: Passport,
    facts: PrincipalFacts,
}

impl AppPrincipal {
    pub fn new(user: Uuid, org: Uuid, scopes: Vec<String>, super_admin: bool) -> Self {
        let mut map = serde_json::Map::new();
        map.insert(
            ORG_CLAIM.to_string(),
            serde_json::Value::String(org.to_string()),
        );
        map.insert(
            SCOPES_CLAIM.to_string(),
            serde_json::Value::Array(
                scopes
                    .iter()
                    .cloned()
                    .map(serde_json::Value::String)
                    .collect(),
            ),
        );
        Self {
            id: PrincipalId::from(user),
            org,
            scopes,
            super_admin,
            passport: Passport::human(
                user,
                super_admin,
                true,
                br_core_auth::AuthMethod::Jwt,
                None,
                PassportClaims::from_map(map),
            ),
            facts: PrincipalFacts::new(),
        }
    }

    pub fn from_actor(actor: Actor) -> Self {
        let passport = match actor {
            Actor::Service(id) => Passport::service(id.as_uuid(), PassportClaims::new()),
            Actor::Human(id) => Passport::human(
                id.as_uuid(),
                false,
                true,
                br_core_auth::AuthMethod::Jwt,
                None,
                PassportClaims::new(),
            ),
        };
        Self {
            id: PrincipalId::from(actor.id()),
            org: Uuid::nil(),
            scopes: Vec::new(),
            super_admin: false,
            passport,
            facts: PrincipalFacts::new(),
        }
    }

    pub fn is_service_sender(&self) -> bool {
        self.passport.service_account_id().is_some()
    }

    pub fn with_fact<F: Any + Send + Sync>(mut self, fact: F) -> Self {
        self.facts.insert(fact);
        self
    }

    pub fn user(&self) -> Uuid {
        self.id.as_uuid()
    }

    pub fn org(&self) -> Uuid {
        self.org
    }

    pub fn facts(&self) -> &PrincipalFacts {
        &self.facts
    }

    pub fn facts_mut(&mut self) -> &mut PrincipalFacts {
        &mut self.facts
    }

    pub fn is_super_admin(&self) -> bool {
        self.super_admin
    }

    pub fn has_scope(&self, scope: &str) -> bool {
        self.super_admin || self.scopes.iter().any(|c| c == scope)
    }
}

impl Principal for AppPrincipal {
    fn id(&self) -> PrincipalId {
        self.id
    }

    fn passport(&self) -> &Passport {
        &self.passport
    }
}

impl PassportPrincipal for AppPrincipal {
    fn from_passport(
        _pg: &PgPool,
        passport: Passport,
    ) -> BoxFuture<'_, Result<Self, PrincipalRejected>> {
        Box::pin(async move {
            let user = passport
                .user_id()
                .ok_or_else(|| PrincipalRejected::new("a human passport is required"))?;
            let org = passport
                .claim::<Uuid>(ORG_CLAIM)
                .ok_or_else(|| PrincipalRejected::new("the passport carries no org claim"))?;
            let scopes = passport
                .claim::<Vec<String>>(SCOPES_CLAIM)
                .unwrap_or_default();
            Ok(AppPrincipal {
                id: PrincipalId::from(user),
                org,
                scopes,
                super_admin: passport.is_super_admin(),
                passport,
                facts: PrincipalFacts::new(),
            })
        })
    }
}

pub struct AppPrincipalResolver;

impl PrincipalResolver<AppPrincipal> for AppPrincipalResolver {
    fn resolve<'a>(
        &'a self,
        _pg: &'a PgPool,
        current: &'a AppPrincipal,
    ) -> BoxFuture<'a, Result<Option<AppPrincipal>, EngineError>> {
        Box::pin(async move {
            Ok(Some(AppPrincipal {
                id: current.id,
                org: current.org,
                scopes: current.scopes.clone(),
                super_admin: current.super_admin,
                passport: current.passport.clone(),
                facts: PrincipalFacts::new(),
            }))
        })
    }
}

pub struct AppRls;

impl RlsApplier<AppPrincipal> for AppRls {
    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        principal: &'a AppPrincipal,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("SELECT set_config($1, $2, true), set_config($3, $4, true)")
                .bind(USER_SESSION_VAR)
                .bind(principal.user().to_string())
                .bind(ORG_SESSION_VAR)
                .bind(principal.org.to_string())
                .execute(&mut *conn)
                .await?;
            Ok(())
        })
    }
}
