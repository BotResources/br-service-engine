use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use super::aggregate::{EVENT_VERSION, LedgerEvent, LedgerState, upcast};

pub struct LedgerAggregate(pub LedgerState);

pub struct LedgerStore;

impl Persistence for LedgerStore {
    type Aggregate = LedgerAggregate;
    type Key = Uuid;
    type Event = LedgerEvent;

    const STYLE: PersistenceStyle = PersistenceStyle::FullEda;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<LedgerAggregate>, EngineError>> {
        Box::pin(async move {
            let Some(mut state) = select_snapshot(conn, key).await? else {
                return Ok(None);
            };
            for (seq, version, payload) in select_events_after(conn, key, state.version).await? {
                let event = upcast(version, &payload)?;
                state.apply(&event);
                state.version = seq;
            }
            state.check_hydrated()?;
            Ok(Some(LedgerAggregate(state)))
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        ledger: &'a LedgerAggregate,
        events: &'a [LedgerEvent],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            append_events(conn, &ledger.0, events).await?;
            upsert_snapshot(conn, &ledger.0).await
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        ledger: &'a LedgerAggregate,
        events: &'a [LedgerEvent],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            append_events(conn, &ledger.0, events).await?;
            insert_snapshot(conn, &ledger.0).await
        })
    }
}

async fn select_snapshot(
    conn: &mut PgConnection,
    key: &Uuid,
) -> Result<Option<LedgerState>, EngineError> {
    let row = sqlx::query(
        "SELECT id, org_id, total, last_author, version FROM ledger_snapshot WHERE id = $1 \
         FOR UPDATE",
    )
    .bind(key)
    .fetch_optional(conn)
    .await?;
    Ok(row.map(|row| {
        LedgerState::from_snapshot(
            row.get("id"),
            row.get("org_id"),
            row.get("total"),
            row.get("last_author"),
            row.get("version"),
        )
    }))
}

async fn select_events_after(
    conn: &mut PgConnection,
    key: &Uuid,
    after: i64,
) -> Result<Vec<(i64, i32, serde_json::Value)>, EngineError> {
    let rows = sqlx::query(
        "SELECT seq, version, payload FROM ledger_event WHERE id = $1 AND seq > $2 ORDER BY seq",
    )
    .bind(key)
    .bind(after)
    .fetch_all(conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.get::<i64, _>("seq"),
                row.get::<i32, _>("version"),
                row.get::<serde_json::Value, _>("payload"),
            )
        })
        .collect())
}

async fn append_events(
    conn: &mut PgConnection,
    state: &LedgerState,
    events: &[LedgerEvent],
) -> Result<(), EngineError> {
    let base = state.base_version();
    for (offset, event) in events.iter().enumerate() {
        let seq = base + offset as i64 + 1;
        let author = match event {
            LedgerEvent::Recorded { author, .. } => *author,
        };
        let payload = serde_json::to_value(event).map_err(|source| EngineError::Encode {
            what: "ledger event",
            source,
        })?;
        sqlx::query(
            "INSERT INTO ledger_event (id, seq, version, author, payload) VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(state.id)
        .bind(seq)
        .bind(EVENT_VERSION)
        .bind(author)
        .bind(&payload)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

async fn insert_snapshot(conn: &mut PgConnection, state: &LedgerState) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO ledger_snapshot (id, org_id, total, last_author, version) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(state.id)
    .bind(state.org_id)
    .bind(state.total)
    .bind(state.last_author)
    .bind(state.version)
    .execute(conn)
    .await?;
    Ok(())
}

async fn upsert_snapshot(conn: &mut PgConnection, state: &LedgerState) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO ledger_snapshot (id, org_id, total, last_author, version) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (id) DO UPDATE SET total = EXCLUDED.total, last_author = EXCLUDED.last_author, \
           version = EXCLUDED.version",
    )
    .bind(state.id)
    .bind(state.org_id)
    .bind(state.total)
    .bind(state.last_author)
    .bind(state.version)
    .execute(conn)
    .await?;
    Ok(())
}

impl Aggregate for LedgerAggregate {
    type Store = LedgerStore;

    fn key(&self) -> Uuid {
        self.0.id
    }

    fn pending_events(&self) -> &[LedgerEvent] {
        self.0.pending()
    }
}

pub async fn all_ledger_ids(pg: &PgPool) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT id FROM ledger_snapshot")
        .fetch_all(pg)
        .await?;
    Ok(rows.iter().map(|row| row.get::<Uuid, _>("id")).collect())
}

pub async fn snapshot_of(
    pg: &PgPool,
    id: Uuid,
) -> Result<Option<(i64, Option<Uuid>)>, EngineError> {
    let row = sqlx::query("SELECT total, last_author FROM ledger_snapshot WHERE id = $1")
        .bind(id)
        .fetch_optional(pg)
        .await?;
    Ok(row.map(|row| {
        (
            row.get::<i64, _>("total"),
            row.get::<Option<Uuid>, _>("last_author"),
        )
    }))
}

pub async fn snapshot_of_conn(
    conn: &mut PgConnection,
    id: Uuid,
) -> Result<Option<(i64, Option<Uuid>)>, EngineError> {
    let row = sqlx::query("SELECT total, last_author FROM ledger_snapshot WHERE id = $1")
        .bind(id)
        .fetch_optional(conn)
        .await?;
    Ok(row.map(|row| {
        (
            row.get::<i64, _>("total"),
            row.get::<Option<Uuid>, _>("last_author"),
        )
    }))
}

pub async fn erase_author(conn: &mut PgConnection, person: Uuid) -> Result<u64, EngineError> {
    let subjects: Vec<Uuid> = sqlx::query("SELECT DISTINCT id FROM ledger_event WHERE author = $1")
        .bind(person)
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|row| row.get::<Uuid, _>("id"))
        .collect();
    sqlx::query(
        "UPDATE ledger_event \
         SET author = $2, payload = jsonb_set(payload, '{author}', to_jsonb($2::uuid)) \
         WHERE author = $1",
    )
    .bind(person)
    .bind(Uuid::nil())
    .execute(&mut *conn)
    .await?;
    sqlx::query("UPDATE ledger_snapshot SET last_author = $2 WHERE last_author = $1")
        .bind(person)
        .bind(Uuid::nil())
        .execute(&mut *conn)
        .await?;
    Ok(subjects.len() as u64)
}
