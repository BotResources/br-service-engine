use std::marker::PhantomData;

use futures_util::future::BoxFuture;
use sqlx::{PgConnection, PgPool, Row};

use crate::error::EngineError;
use crate::full_eda::read::{
    decode_snapshot, read_aggregates, select_events_after, select_snapshot,
};
use crate::full_eda::{EventSourced, decode_key_text, encode_key_text};
use crate::persistence::{Persistence, PersistenceStyle, RowBatch};
use crate::schema::{TABLE_EVENT_LOG, TABLE_EVENT_SNAPSHOT};

pub struct FullEda<T>(PhantomData<T>);

impl<T: EventSourced> Persistence for FullEda<T> {
    type Aggregate = T;
    type Key = T::Key;
    type Event = T::Event;

    const STYLE: PersistenceStyle = PersistenceStyle::FullEda;

    fn read_many<'a>(conn: &'a mut PgConnection, keys: &'a [T::Key]) -> RowBatch<'a, T::Key, T> {
        Box::pin(read_aggregates::<T>(conn, keys))
    }

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a T::Key,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let key_text = encode_key_text(key)?;
            sqlx::query(&format!(
                "SELECT 1 FROM {TABLE_EVENT_SNAPSHOT} WHERE noun = $1 AND key = $2 FOR UPDATE"
            ))
            .bind(T::NOUN.as_str())
            .bind(&key_text)
            .fetch_optional(conn)
            .await?;
            Ok(())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a T,
        events: &'a [T::Event],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let key_text = encode_key_text(&aggregate.key())?;
            let new_version = aggregate.version();
            let base = new_version - events.len() as i64;
            append_events::<T>(conn, &key_text, base, events).await?;
            if events.is_empty() {
                return Ok(());
            }
            if crosses_cadence(base, new_version, T::SNAPSHOT_EVERY) {
                upsert_snapshot::<T>(conn, &key_text, aggregate).await?;
            }
            Ok(())
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a T,
        events: &'a [T::Event],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let key_text = encode_key_text(&aggregate.key())?;
            let base = aggregate.version() - events.len() as i64;
            append_events::<T>(conn, &key_text, base, events).await?;
            insert_snapshot::<T>(conn, &key_text, aggregate).await
        })
    }
}

fn crosses_cadence(base: i64, new_version: i64, every: i64) -> bool {
    if every <= 1 {
        return true;
    }
    base / every != new_version / every
}

pub(crate) async fn resnapshot_from_log<T: EventSourced>(
    conn: &mut PgConnection,
    key_text: &str,
) -> Result<(), EngineError> {
    let Some((_, state)) = select_snapshot(conn, T::NOUN.as_str(), key_text).await? else {
        return Ok(());
    };
    let mut aggregate = T::from_snapshot(decode_snapshot::<T>(state)?).genesis();
    aggregate.set_version(0);
    for (seq, event_version, payload) in
        select_events_after(conn, T::NOUN.as_str(), key_text, 0).await?
    {
        let event = T::upcast(event_version, &payload)?;
        aggregate.apply(&event);
        aggregate.set_version(seq);
    }
    aggregate.check_hydrated()?;
    upsert_snapshot::<T>(conn, key_text, &aggregate).await
}

pub(crate) async fn append_events<T: EventSourced>(
    conn: &mut PgConnection,
    key_text: &str,
    base: i64,
    events: &[T::Event],
) -> Result<(), EngineError> {
    for (offset, event) in events.iter().enumerate() {
        let seq = base + offset as i64 + 1;
        let payload = serde_json::to_value(event).map_err(|source| EngineError::Encode {
            what: "full-eda event",
            source,
        })?;
        sqlx::query(&format!(
            "INSERT INTO {TABLE_EVENT_LOG} (noun, key, seq, version, payload) \
             VALUES ($1, $2, $3, $4, $5)"
        ))
        .bind(T::NOUN.as_str())
        .bind(key_text)
        .bind(seq)
        .bind(T::EVENT_VERSION)
        .bind(&payload)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

pub(crate) async fn upsert_snapshot<T: EventSourced>(
    conn: &mut PgConnection,
    key_text: &str,
    aggregate: &T,
) -> Result<(), EngineError> {
    let state = snapshot_value(aggregate)?;
    sqlx::query(&format!(
        "INSERT INTO {TABLE_EVENT_SNAPSHOT} (noun, key, version, state) VALUES ($1, $2, $3, $4) \
         ON CONFLICT (noun, key) DO UPDATE SET version = EXCLUDED.version, state = EXCLUDED.state"
    ))
    .bind(T::NOUN.as_str())
    .bind(key_text)
    .bind(aggregate.version())
    .bind(&state)
    .execute(conn)
    .await?;
    Ok(())
}

async fn insert_snapshot<T: EventSourced>(
    conn: &mut PgConnection,
    key_text: &str,
    aggregate: &T,
) -> Result<(), EngineError> {
    let state = snapshot_value(aggregate)?;
    sqlx::query(&format!(
        "INSERT INTO {TABLE_EVENT_SNAPSHOT} (noun, key, version, state) VALUES ($1, $2, $3, $4)"
    ))
    .bind(T::NOUN.as_str())
    .bind(key_text)
    .bind(aggregate.version())
    .bind(&state)
    .execute(conn)
    .await?;
    Ok(())
}

fn snapshot_value<T: EventSourced>(aggregate: &T) -> Result<serde_json::Value, EngineError> {
    serde_json::to_value(aggregate.to_snapshot()).map_err(|source| EngineError::Encode {
        what: "full-eda snapshot",
        source,
    })
}

pub(crate) async fn snapshot_keys<T: EventSourced>(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<T::Key>, EngineError> {
    let rows = sqlx::query(&format!(
        "SELECT key FROM {TABLE_EVENT_SNAPSHOT} WHERE noun = $1 LIMIT $2"
    ))
    .bind(T::NOUN.as_str())
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.iter()
        .map(|row| decode_key_text::<T::Key>(row.try_get::<&str, _>("key")?))
        .collect()
}
