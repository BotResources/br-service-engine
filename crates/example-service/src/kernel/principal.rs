use br_core_auth::{Passport, PassportClaims};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::principal::{Principal, PrincipalId, PrincipalResolver, RlsApplier};
use service_engine::{PassportPrincipal, PrincipalRejected};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

pub const ORG_CLAIM: &str = "org";
pub const USER_SESSION_VAR: &str = "app.current_user_id";
pub const ORG_SESSION_VAR: &str = "app.current_org_id";

pub const SCOPES_CLAIM: &str = "scopes";

#[derive(Debug, Clone)]
pub struct AppPrincipal {
    id: PrincipalId,
    org: Uuid,
    boards: Vec<Uuid>,
    scopes: Vec<String>,
    super_admin: bool,
    passport: Passport,
}

impl AppPrincipal {
    pub fn new(
        user: Uuid,
        org: Uuid,
        boards: Vec<Uuid>,
        scopes: Vec<String>,
        super_admin: bool,
    ) -> Self {
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
            boards,
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
        }
    }

    pub fn user(&self) -> Uuid {
        self.id.as_uuid()
    }

    pub fn org(&self) -> Uuid {
        self.org
    }

    pub fn boards(&self) -> &[Uuid] {
        &self.boards
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

async fn boards_of(_pg: &PgPool, _user: Uuid) -> Result<Vec<Uuid>, EngineError> {
    #[cfg(feature = "board")]
    {
        return crate::slices::board::boards_of(_pg, _user).await;
    }
    #[cfg(not(feature = "board"))]
    {
        Ok(Vec::new())
    }
}

impl PassportPrincipal for AppPrincipal {
    fn from_passport(
        pg: &PgPool,
        passport: Passport,
    ) -> BoxFuture<'_, Result<Self, PrincipalRejected>> {
        Box::pin(async move {
            let user = passport
                .user_id()
                .ok_or_else(|| PrincipalRejected::new("a human passport is required"))?;
            let org = passport
                .claim::<Uuid>(ORG_CLAIM)
                .ok_or_else(|| PrincipalRejected::new("the passport carries no org claim"))?;
            let boards = boards_of(pg, user)
                .await
                .map_err(|error| PrincipalRejected::new(error.to_string()))?;
            let scopes = passport
                .claim::<Vec<String>>(SCOPES_CLAIM)
                .unwrap_or_default();
            Ok(AppPrincipal {
                id: PrincipalId::from(user),
                org,
                boards,
                scopes,
                super_admin: passport.is_super_admin(),
                passport,
            })
        })
    }
}

pub struct AppPrincipalResolver;

impl PrincipalResolver<AppPrincipal> for AppPrincipalResolver {
    fn resolve<'a>(
        &'a self,
        pg: &'a PgPool,
        current: &'a AppPrincipal,
    ) -> BoxFuture<'a, Result<Option<AppPrincipal>, EngineError>> {
        Box::pin(async move {
            let boards = boards_of(pg, current.user()).await?;
            Ok(Some(AppPrincipal {
                id: current.id,
                org: current.org,
                boards,
                scopes: current.scopes.clone(),
                super_admin: current.super_admin,
                passport: current.passport.clone(),
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
