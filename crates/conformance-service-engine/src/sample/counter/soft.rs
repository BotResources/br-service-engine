use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::name::NounName;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use service_engine::wire::Noun;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::sample::counter::domain::CounterState;
use crate::sample::counter::event::{CounterEvent, EVENT_VERSION};
use crate::sample::counter::slice::CounterAggregate;

pub struct SoftCounterNoun;

impl Noun for SoftCounterNoun {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("counter_soft");
}

#[derive(Debug)]
pub struct SoftCounter(pub CounterState);

pub struct SoftCounterStore;

impl Persistence for SoftCounterStore {
    type Aggregate = SoftCounter;
    type Key = Uuid;
    type Event = CounterEvent;

    const STYLE: PersistenceStyle = PersistenceStyle::SoftEda;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<SoftCounter>, EngineError>> {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT id, tenant, total, closed, version FROM sample_counter_soft WHERE id = $1",
            )
            .bind(key)
            .fetch_optional(conn)
            .await?;
            Ok(row.map(|row| {
                SoftCounter(CounterState::from_row(
                    row.get("id"),
                    row.get("tenant"),
                    row.get("total"),
                    row.get("closed"),
                    None,
                    row.get("version"),
                ))
            }))
        })
    }

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("SELECT id FROM sample_counter_soft WHERE id = $1 FOR UPDATE")
                .bind(key)
                .fetch_optional(conn)
                .await?;
            Ok(())
        })
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, SoftCounter)>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(
                "SELECT id, tenant, total, closed, version FROM sample_counter_soft \
                 WHERE id = ANY($1)",
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
                        None,
                        row.get("version"),
                    );
                    (state.key, SoftCounter(state))
                })
                .collect())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a SoftCounter,
        events: &'a [CounterEvent],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            upsert(conn, &aggregate.0).await?;
            append_facts(conn, &aggregate.0, events).await
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a SoftCounter,
        events: &'a [CounterEvent],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO sample_counter_soft (id, tenant, total, closed, version) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(aggregate.0.key)
            .bind(aggregate.0.tenant)
            .bind(aggregate.0.total)
            .bind(aggregate.0.closed)
            .bind(aggregate.0.version)
            .execute(&mut *conn)
            .await?;
            append_facts(conn, &aggregate.0, events).await
        })
    }
}

async fn upsert(conn: &mut PgConnection, state: &CounterState) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO sample_counter_soft (id, tenant, total, closed, version) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (id) DO UPDATE SET tenant = EXCLUDED.tenant, total = EXCLUDED.total, \
           closed = EXCLUDED.closed, version = EXCLUDED.version",
    )
    .bind(state.key)
    .bind(state.tenant)
    .bind(state.total)
    .bind(state.closed)
    .bind(state.version)
    .execute(conn)
    .await?;
    Ok(())
}

async fn append_facts(
    conn: &mut PgConnection,
    state: &CounterState,
    events: &[CounterEvent],
) -> Result<(), EngineError> {
    let base = state.base_version();
    for (offset, event) in events.iter().enumerate() {
        let seq = base + offset as i64 + 1;
        let payload = serde_json::to_value(event).map_err(|source| EngineError::Encode {
            what: "counter fact",
            source,
        })?;
        sqlx::query(
            "INSERT INTO sample_counter_soft_fact (counter, seq, version, payload) \
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

impl Aggregate for SoftCounter {
    type Store = SoftCounterStore;

    fn key(&self) -> Uuid {
        self.0.key
    }

    fn pending_events(&self) -> &[CounterEvent] {
        self.0.pending()
    }
}

impl CounterAggregate for SoftCounter {
    type Noun = SoftCounterNoun;

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
