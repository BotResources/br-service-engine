use sqlx::PgPool;

use crate::error::EngineError;

pub async fn assert_owner_posture(owner: &PgPool) -> Result<(), EngineError> {
    let (role, bypasses): (String, bool) = sqlx::query_as(
        "SELECT rolname::text, rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user",
    )
    .fetch_one(owner)
    .await?;
    if bypasses {
        Ok(())
    } else {
        Err(EngineError::OwnerSubjectToRls { role })
    }
}
