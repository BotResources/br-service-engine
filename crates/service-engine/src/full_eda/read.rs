use std::collections::HashMap;

use sqlx::postgres::PgRow;
use sqlx::{PgConnection, Row};

use crate::error::EngineError;
use crate::full_eda::{EventSourced, encode_key_text};
use crate::schema::{TABLE_EVENT_LOG, TABLE_EVENT_SNAPSHOT};

pub(super) type LoggedEvent = (i64, i32, serde_json::Value);

type Snapshots = Vec<(String, i64, serde_json::Value)>;

pub(super) async fn read_aggregates<T: EventSourced>(
    conn: &mut PgConnection,
    keys: &[T::Key],
) -> Result<Vec<(T::Key, T)>, EngineError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut requested = HashMap::with_capacity(keys.len());
    for key in keys {
        requested.insert(encode_key_text(key)?, key.clone());
    }
    let texts: Vec<String> = requested.keys().cloned().collect();
    let snapshots = select_snapshots(conn, T::NOUN.as_str(), &texts).await?;
    if snapshots.is_empty() {
        return Ok(Vec::new());
    }
    let mut logged = select_events_after_each(conn, T::NOUN.as_str(), &snapshots).await?;
    let mut aggregates = Vec::with_capacity(snapshots.len());
    for (key_text, version, state) in snapshots {
        if let Some(key) = requested.remove(&key_text) {
            let events = logged.remove(&key_text).unwrap_or_default();
            aggregates.push((key, hydrate::<T>(version, state, events)?));
        }
    }
    Ok(aggregates)
}

fn hydrate<T: EventSourced>(
    version: i64,
    state: serde_json::Value,
    events: Vec<LoggedEvent>,
) -> Result<T, EngineError> {
    let mut aggregate = T::from_snapshot(decode_snapshot::<T>(state)?);
    aggregate.set_version(version);
    for (seq, event_version, payload) in events {
        let event = T::upcast(event_version, &payload)?;
        aggregate.apply(&event);
        aggregate.set_version(seq);
    }
    aggregate.check_hydrated()?;
    Ok(aggregate)
}

pub(super) fn decode_snapshot<T: EventSourced>(
    state: serde_json::Value,
) -> Result<T::Snapshot, EngineError> {
    serde_json::from_value(state).map_err(|source| EngineError::Decode {
        what: "full-eda snapshot",
        source,
    })
}

async fn select_snapshots(
    conn: &mut PgConnection,
    noun: &str,
    key_texts: &[String],
) -> Result<Snapshots, EngineError> {
    let rows = sqlx::query(&format!(
        "SELECT key, version, state FROM {TABLE_EVENT_SNAPSHOT} WHERE noun = $1 AND key = ANY($2)"
    ))
    .bind(noun)
    .bind(key_texts)
    .fetch_all(conn)
    .await?;
    rows.iter()
        .map(|row| {
            Ok((
                row.try_get("key")?,
                row.try_get("version")?,
                row.try_get("state")?,
            ))
        })
        .collect()
}

async fn select_events_after_each(
    conn: &mut PgConnection,
    noun: &str,
    snapshots: &Snapshots,
) -> Result<HashMap<String, Vec<LoggedEvent>>, EngineError> {
    let (key_texts, versions): (Vec<String>, Vec<i64>) = snapshots
        .iter()
        .map(|(key_text, version, _)| (key_text.clone(), *version))
        .unzip();
    let rows = sqlx::query(&format!(
        "SELECT e.key, e.seq, e.version, e.payload FROM {TABLE_EVENT_LOG} e \
         JOIN unnest($2::text[], $3::bigint[]) AS s(key, after) ON e.key = s.key \
         WHERE e.noun = $1 AND e.seq > s.after ORDER BY e.key, e.seq"
    ))
    .bind(noun)
    .bind(&key_texts)
    .bind(&versions)
    .fetch_all(conn)
    .await?;
    let mut logged: HashMap<String, Vec<LoggedEvent>> = HashMap::new();
    for row in &rows {
        logged
            .entry(row.try_get("key")?)
            .or_default()
            .push(logged_event(row)?);
    }
    Ok(logged)
}

pub(super) async fn select_snapshot(
    conn: &mut PgConnection,
    noun: &str,
    key_text: &str,
) -> Result<Option<(i64, serde_json::Value)>, EngineError> {
    let row = sqlx::query(&format!(
        "SELECT version, state FROM {TABLE_EVENT_SNAPSHOT} WHERE noun = $1 AND key = $2"
    ))
    .bind(noun)
    .bind(key_text)
    .fetch_optional(conn)
    .await?;
    row.map(|row| Ok((row.try_get("version")?, row.try_get("state")?)))
        .transpose()
}

pub(super) async fn select_events_after(
    conn: &mut PgConnection,
    noun: &str,
    key_text: &str,
    after: i64,
) -> Result<Vec<LoggedEvent>, EngineError> {
    let rows = sqlx::query(&format!(
        "SELECT seq, version, payload FROM {TABLE_EVENT_LOG} \
         WHERE noun = $1 AND key = $2 AND seq > $3 ORDER BY seq"
    ))
    .bind(noun)
    .bind(key_text)
    .bind(after)
    .fetch_all(conn)
    .await?;
    rows.iter().map(logged_event).collect()
}

fn logged_event(row: &PgRow) -> Result<LoggedEvent, EngineError> {
    Ok((
        row.try_get("seq")?,
        row.try_get("version")?,
        row.try_get("payload")?,
    ))
}
