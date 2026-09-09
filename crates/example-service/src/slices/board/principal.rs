use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use sqlx::PgPool;
use uuid::Uuid;

use crate::kernel::AppPrincipal;

pub struct BoardMemberships(pub Vec<Uuid>);

pub fn load_board_memberships<'a>(
    pg: &'a PgPool,
    principal: &'a mut AppPrincipal,
) -> BoxFuture<'a, Result<(), EngineError>> {
    Box::pin(async move {
        let boards = super::store::boards_of(pg, principal.user()).await?;
        principal.facts_mut().insert(BoardMemberships(boards));
        Ok(())
    })
}
