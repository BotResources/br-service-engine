use async_graphql::{Context, Object, Result};
use service_engine::Query;
use service_engine::error::EngineError;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::sample::gated::VisibleAssignments;
use crate::sample::principal::SamplePrincipal;

#[derive(Default)]
pub struct SnapshotReads;

#[Object]
impl SnapshotReads {
    async fn sample_notes_across_latch(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
        latch: i32,
    ) -> Result<Option<Vec<String>>> {
        Query::<SamplePrincipal>::new(ctx)?
            .behind::<VisibleAssignments>(&id)
            .read(move |assignment, conn| {
                Box::pin(async move {
                    wait_on(&mut *conn, latch).await?;
                    Ok(sqlx::query_scalar(
                        "SELECT body FROM sample_note WHERE assignment_id = $1 ORDER BY seq",
                    )
                    .bind(assignment.id)
                    .fetch_all(conn)
                    .await?)
                })
            })
            .await
    }

    async fn sample_journal_across_latch(&self, ctx: &Context<'_>, latch: i32) -> Result<Vec<i64>> {
        Query::<SamplePrincipal>::new(ctx)?
            .read_under_rls(move |conn| {
                Box::pin(async move {
                    let before = journal_length(&mut *conn).await?;
                    wait_on(&mut *conn, latch).await?;
                    let after = journal_length(conn).await?;
                    Ok(vec![before, after])
                })
            })
            .await
    }
}

async fn wait_on(conn: &mut PgConnection, latch: i32) -> Result<(), EngineError> {
    sqlx::query("SELECT pg_advisory_xact_lock_shared($1::bigint)")
        .bind(latch)
        .execute(conn)
        .await?;
    Ok(())
}

async fn journal_length(conn: &mut PgConnection) -> Result<i64, EngineError> {
    Ok(sqlx::query_scalar("SELECT count(*) FROM sample_assignment")
        .fetch_one(conn)
        .await?)
}
