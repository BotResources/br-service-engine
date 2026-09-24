use service_engine::error::EngineError;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::sample::counter::domain::CounterState;
use crate::sample::counter::full::resnapshot;
use crate::sample::counter::full_read::{replay_tails, select_snapshots};

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

    let origins = select_snapshots(conn, &affected)
        .await?
        .into_iter()
        .map(|snapshot| CounterState::open(snapshot.key, snapshot.tenant))
        .collect();
    for rebuilt in replay_tails(conn, origins).await? {
        resnapshot(conn, &rebuilt).await?;
    }

    Ok(affected)
}
