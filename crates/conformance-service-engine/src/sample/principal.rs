use br_core_auth::{AuthMethod, Passport, PassportClaims};
use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::principal::{Principal, PrincipalId, PrincipalResolver, RlsApplier};
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

pub const TENANT_SESSION_VAR: &str = "app.current_tenant_id";
pub const USER_SESSION_VAR: &str = "app.current_user_id";

#[derive(Debug, Clone)]
pub struct SamplePrincipal {
    id: PrincipalId,
    tenant: Uuid,
    passport: Passport,
}

impl SamplePrincipal {
    pub fn new(user_id: Uuid, tenant: Uuid) -> Self {
        Self {
            id: PrincipalId::from(user_id),
            tenant,
            passport: Passport::human(
                user_id,
                false,
                true,
                AuthMethod::Jwt,
                None,
                PassportClaims::new(),
            ),
        }
    }

    pub fn tenant(&self) -> Uuid {
        self.tenant
    }
}

impl Principal for SamplePrincipal {
    fn id(&self) -> PrincipalId {
        self.id
    }

    fn passport(&self) -> &Passport {
        &self.passport
    }
}

pub struct SampleRls;

impl RlsApplier<SamplePrincipal> for SampleRls {
    fn apply<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        principal: &'a SamplePrincipal,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("SELECT set_config($1, $2, true), set_config($3, $4, true)")
                .bind(USER_SESSION_VAR)
                .bind(principal.id().as_uuid().to_string())
                .bind(TENANT_SESSION_VAR)
                .bind(principal.tenant.to_string())
                .execute(&mut *conn)
                .await?;
            Ok(())
        })
    }
}

pub struct FailingPrincipalResolver;

impl PrincipalResolver<SamplePrincipal> for FailingPrincipalResolver {
    fn resolve<'a>(
        &'a self,
        _pg: &'a PgPool,
        _current: &'a SamplePrincipal,
    ) -> BoxFuture<'a, Result<Option<SamplePrincipal>, EngineError>> {
        Box::pin(async move {
            Err(EngineError::Service(
                "the principal store is unreachable for the duration of the test".into(),
            ))
        })
    }
}

pub struct SamplePrincipalResolver;

impl PrincipalResolver<SamplePrincipal> for SamplePrincipalResolver {
    fn resolve<'a>(
        &'a self,
        pg: &'a PgPool,
        current: &'a SamplePrincipal,
    ) -> BoxFuture<'a, Result<Option<SamplePrincipal>, EngineError>> {
        Box::pin(async move {
            let row = sqlx::query("SELECT tenant_id FROM sample_member WHERE user_id = $1")
                .bind(current.id().as_uuid())
                .fetch_optional(pg)
                .await?;
            Ok(row.map(|row| {
                SamplePrincipal::new(current.id().as_uuid(), row.get::<Uuid, _>("tenant_id"))
            }))
        })
    }
}
