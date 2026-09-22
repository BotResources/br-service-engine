use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::name::NounName;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use service_engine::wire::Noun;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::sample::counter::domain::CounterState;
use crate::sample::counter::event::{CounterEvent, EVENT_VERSION, upcast};
use crate::sample::counter::slice::CounterAggregate;

pub struct FullCounterNoun;

impl Noun for FullCounterNoun {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("counter_full");
}

#[derive(Debug, Clone)]
pub struct FullCounter(pub CounterState);

pub struct FullCounterStore;

impl Persistence for FullCounterStore {
    type Aggregate = FullCounter;
    type Key = Uuid;
    type Event = CounterEvent;

    const STYLE: PersistenceStyle = PersistenceStyle::FullEda;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<FullCounter>, EngineError>> {
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
            Ok(Some(FullCounter(state)))
        })
    }

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("SELECT id FROM sample_counter_full_snapshot WHERE id = $1 FOR UPDATE")
                .bind(key)
                .fetch_optional(conn)
                .await?;
            Ok(())
        })
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, FullCounter)>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(
                "SELECT id, tenant, total, closed, last_author, version \
                 FROM sample_counter_full_snapshot WHERE id = ANY($1)",
            )
            .bind(keys)
            .fetch_all(conn)
            .await?;
            Ok(rows
                .into_iter()
                .map(|row| {
                    let state = CounterState::from_row(
                        row.get("id"),
                        row.get("tenant"),
                        row.get("total"),
                        row.get("closed"),
                        row.get("last_author"),
                        row.get("version"),
                    );
                    (state.key, FullCounter(state))
                })
                .collect())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a FullCounter,
        events: &'a [CounterEvent],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            append_events(conn, &aggregate.0, events).await?;
            upsert_snapshot(conn, &aggregate.0).await
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a FullCounter,
        events: &'a [CounterEvent],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            append_events(conn, &aggregate.0, events).await?;
            insert_snapshot(conn, &aggregate.0).await
        })
    }
}

pub(crate) async fn select_snapshot(
    conn: &mut PgConnection,
    key: &Uuid,
) -> Result<Option<CounterState>, EngineError> {
    let row = sqlx::query(
        "SELECT id, tenant, total, closed, last_author, version \
         FROM sample_counter_full_snapshot WHERE id = $1",
    )
    .bind(key)
    .fetch_optional(conn)
    .await?;
    Ok(row.map(|row| {
        CounterState::from_row(
            row.get("id"),
            row.get("tenant"),
            row.get("total"),
            row.get("closed"),
            row.get("last_author"),
            row.get("version"),
        )
    }))
}

pub(crate) async fn select_events_after(
    conn: &mut PgConnection,
    key: &Uuid,
    after: i64,
) -> Result<Vec<(i64, i32, serde_json::Value)>, EngineError> {
    let rows = sqlx::query(
        "SELECT seq, version, payload FROM sample_counter_full_event \
         WHERE counter = $1 AND seq > $2 ORDER BY seq",
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
    state: &CounterState,
    events: &[CounterEvent],
) -> Result<(), EngineError> {
    let base = state.base_version();
    for (offset, event) in events.iter().enumerate() {
        let seq = base + offset as i64 + 1;
        let payload = serde_json::to_value(event).map_err(|source| EngineError::Encode {
            what: "counter event",
            source,
        })?;
        sqlx::query(
            "INSERT INTO sample_counter_full_event (counter, seq, version, payload) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(state.key)
        .bind(seq)
        .bind(EVENT_VERSION)
        .bind(&payload)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

pub(crate) async fn resnapshot(
    conn: &mut PgConnection,
    state: &CounterState,
) -> Result<(), EngineError> {
    upsert_snapshot(conn, state).await
}

async fn upsert_snapshot(conn: &mut PgConnection, state: &CounterState) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO sample_counter_full_snapshot \
           (id, tenant, total, closed, last_author, version) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (id) DO UPDATE SET tenant = EXCLUDED.tenant, total = EXCLUDED.total, \
           closed = EXCLUDED.closed, last_author = EXCLUDED.last_author, version = EXCLUDED.version",
    )
    .bind(state.key)
    .bind(state.tenant)
    .bind(state.total)
    .bind(state.closed)
    .bind(&state.last_author)
    .bind(state.version)
    .execute(conn)
    .await?;
    Ok(())
}

async fn insert_snapshot(conn: &mut PgConnection, state: &CounterState) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO sample_counter_full_snapshot \
           (id, tenant, total, closed, last_author, version) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(state.key)
    .bind(state.tenant)
    .bind(state.total)
    .bind(state.closed)
    .bind(&state.last_author)
    .bind(state.version)
    .execute(conn)
    .await?;
    Ok(())
}

pub async fn replay_from_scratch(
    conn: &mut PgConnection,
    key: &Uuid,
    tenant: Uuid,
) -> Result<CounterState, EngineError> {
    let mut state = CounterState::open(*key, tenant);
    for (seq, version, payload) in select_events_after(conn, key, 0).await? {
        let event = upcast(version, &payload)?;
        state.apply(&event);
        state.version = seq;
    }
    state.check_hydrated()?;
    Ok(state)
}

impl Aggregate for FullCounter {
    type Store = FullCounterStore;

    fn key(&self) -> Uuid {
        self.0.key
    }

    fn pending_events(&self) -> &[CounterEvent] {
        self.0.pending()
    }
}

impl CounterAggregate for FullCounter {
    type Noun = FullCounterNoun;

    fn open(key: Uuid, tenant: Uuid) -> Self {
        Self(CounterState::open(key, tenant))
    }

    fn state(&self) -> &CounterState {
        &self.0
    }

    fn state_mut(&mut self) -> &mut CounterState {
        &mut self.0
    }
}
