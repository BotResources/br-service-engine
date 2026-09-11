use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::name::NounName;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use service_engine::wire::Noun;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::sample::counter::domain::CounterState;
use crate::sample::counter::event::CounterEvent;
use crate::sample::counter::slice::CounterAggregate;

pub struct CrudCounterNoun;

impl Noun for CrudCounterNoun {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("counter_crud");
}

#[derive(Debug)]
pub struct CrudCounter(pub CounterState);

pub struct CrudCounterStore;

impl Persistence for CrudCounterStore {
    type Aggregate = CrudCounter;
    type Key = Uuid;
    type Event = CounterEvent;

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn load<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<Option<CrudCounter>, EngineError>> {
        Box::pin(async move {
            let row = sqlx::query(
                "SELECT id, tenant, total, closed FROM sample_counter_crud WHERE id = $1",
            )
            .bind(key)
            .fetch_optional(conn)
            .await?;
            Ok(row.map(|row| {
                CrudCounter(CounterState::from_row(
                    row.get("id"),
                    row.get("tenant"),
                    row.get("total"),
                    row.get("closed"),
                    None,
                    0,
                ))
            }))
        })
    }

    fn lock<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("SELECT id FROM sample_counter_crud WHERE id = $1 FOR UPDATE")
                .bind(key)
                .fetch_optional(conn)
                .await?;
            Ok(())
        })
    }

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, CrudCounter)>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(
                "SELECT id, tenant, total, closed FROM sample_counter_crud WHERE id = ANY($1)",
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
                        0,
                    );
                    (state.key, CrudCounter(state))
                })
                .collect())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a CrudCounter,
        _events: &'a [CounterEvent],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move { upsert(conn, &aggregate.0).await })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a CrudCounter,
        _events: &'a [CounterEvent],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO sample_counter_crud (id, tenant, total, closed) VALUES ($1, $2, $3, $4)",
            )
            .bind(aggregate.0.key)
            .bind(aggregate.0.tenant)
            .bind(aggregate.0.total)
            .bind(aggregate.0.closed)
            .execute(conn)
            .await?;
            Ok(())
        })
    }
}

async fn upsert(conn: &mut PgConnection, state: &CounterState) -> Result<(), EngineError> {
    sqlx::query(
        "INSERT INTO sample_counter_crud (id, tenant, total, closed) VALUES ($1, $2, $3, $4) \
         ON CONFLICT (id) DO UPDATE SET tenant = EXCLUDED.tenant, total = EXCLUDED.total, \
           closed = EXCLUDED.closed",
    )
    .bind(state.key)
    .bind(state.tenant)
    .bind(state.total)
    .bind(state.closed)
    .execute(conn)
    .await?;
    Ok(())
}

impl Aggregate for CrudCounter {
    type Store = CrudCounterStore;

    fn key(&self) -> Uuid {
        self.0.key
    }

    fn pending_events(&self) -> &[CounterEvent] {
        self.0.pending()
    }
}

impl CounterAggregate for CrudCounter {
    type Noun = CrudCounterNoun;

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
