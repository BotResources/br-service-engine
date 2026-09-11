use service_engine::error::EngineError;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::sample::counter::full::{replay_from_scratch, resnapshot, select_snapshot};

pub async fn erase_author(conn: &mut PgConnection, author: &str) -> Result<Vec<Uuid>, EngineError> {
    let affected: Vec<Uuid> = sqlx::query(
        "SELECT DISTINCT counter FROM sample_counter_full_event \
         WHERE payload ->> 'author' = $1 OR payload ->> 'who' = $1",
    )
    .bind(author)
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|row| row.get::<Uuid, _>("counter"))
    .collect();

    sqlx::query(
        "UPDATE sample_counter_full_event \
         SET payload = CASE \
             WHEN payload ? 'author' THEN jsonb_set(payload, '{author}', '\"erased\"') \
             WHEN payload ? 'who' THEN jsonb_set(payload, '{who}', '\"erased\"') \
             ELSE payload END \
         WHERE payload ->> 'author' = $1 OR payload ->> 'who' = $1",
    )
    .bind(author)
    .execute(&mut *conn)
    .await?;

    for counter in &affected {
        let Some(state) = select_snapshot(conn, counter).await? else {
            continue;
        };
        let rebuilt = replay_from_scratch(conn, counter, state.tenant).await?;
        resnapshot(conn, &rebuilt).await?;
    }

    Ok(affected)
}
