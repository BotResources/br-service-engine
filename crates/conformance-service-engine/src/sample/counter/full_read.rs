use std::collections::HashMap;

use service_engine::error::EngineError;
use sqlx::postgres::PgRow;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::sample::counter::domain::CounterState;
use crate::sample::counter::event::upcast;

type LoggedEvent = (i64, i32, serde_json::Value);

pub(crate) async fn hydrate_many(
    conn: &mut PgConnection,
    keys: &[Uuid],
) -> Result<Vec<CounterState>, EngineError> {
    let snapshots = select_snapshots(conn, keys).await?;
    replay_tails(conn, snapshots).await
}

pub(crate) async fn replay_tails(
    conn: &mut PgConnection,
    origins: Vec<CounterState>,
) -> Result<Vec<CounterState>, EngineError> {
    let mut tails = select_events_after_each(conn, &origins).await?;
    origins
        .into_iter()
        .map(|state| {
            let events = tails.remove(&state.key).unwrap_or_default();
            replay(state, events)
        })
        .collect()
}

pub(crate) fn replay(
    mut state: CounterState,
    events: Vec<LoggedEvent>,
) -> Result<CounterState, EngineError> {
    for (seq, version, payload) in events {
        state.apply(&upcast(version, &payload)?);
        state.version = seq;
    }
    state.check_hydrated()?;
    Ok(state)
}

pub(crate) async fn select_snapshots(
    conn: &mut PgConnection,
    keys: &[Uuid],
) -> Result<Vec<CounterState>, EngineError> {
    let rows = sqlx::query(
        "SELECT id, tenant, total, closed, last_author, version \
         FROM sample_counter_full_snapshot WHERE id = ANY($1)",
    )
    .bind(keys)
    .fetch_all(conn)
    .await?;
    Ok(rows.iter().map(snapshot_of).collect())
}

pub(crate) async fn select_events_after(
    conn: &mut PgConnection,
    key: &Uuid,
    after: i64,
) -> Result<Vec<LoggedEvent>, EngineError> {
    let rows = sqlx::query(
        "SELECT seq, version, payload FROM sample_counter_full_event \
         WHERE counter = $1 AND seq > $2 ORDER BY seq",
    )
    .bind(key)
    .bind(after)
    .fetch_all(conn)
    .await?;
    Ok(rows.iter().map(logged_event).collect())
}

async fn select_events_after_each(
    conn: &mut PgConnection,
    snapshots: &[CounterState],
) -> Result<HashMap<Uuid, Vec<LoggedEvent>>, EngineError> {
    let mut tails: HashMap<Uuid, Vec<LoggedEvent>> = HashMap::new();
    if snapshots.is_empty() {
        return Ok(tails);
    }
    let keys: Vec<Uuid> = snapshots.iter().map(|state| state.key).collect();
    let after: Vec<i64> = snapshots.iter().map(|state| state.version).collect();
    let rows = sqlx::query(
        "SELECT e.counter, e.seq, e.version, e.payload FROM sample_counter_full_event e \
         JOIN unnest($1::uuid[], $2::bigint[]) AS s(counter, after) ON e.counter = s.counter \
         WHERE e.seq > s.after ORDER BY e.counter, e.seq",
    )
    .bind(&keys)
    .bind(&after)
    .fetch_all(conn)
    .await?;
    for row in &rows {
        tails
            .entry(row.get("counter"))
            .or_default()
            .push(logged_event(row));
    }
    Ok(tails)
}

fn snapshot_of(row: &PgRow) -> CounterState {
    CounterState::from_row(
        row.get("id"),
        row.get("tenant"),
        row.get("total"),
        row.get("closed"),
        row.get("last_author"),
        row.get("version"),
    )
}

fn logged_event(row: &PgRow) -> LoggedEvent {
    (row.get("seq"), row.get("version"), row.get("payload"))
}
