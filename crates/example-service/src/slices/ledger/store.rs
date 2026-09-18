use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::full_eda::{self, EventSourced, FullEda};
use service_engine::name::NounName;
use service_engine::persistence::Aggregate;
use sqlx::PgPool;
use uuid::Uuid;

use super::aggregate::{EVENT_VERSION, LedgerEvent, LedgerState, upcast};

#[derive(Clone)]
pub struct LedgerAggregate(pub LedgerState);

pub type LedgerStore = FullEda<LedgerAggregate>;

#[derive(Serialize, Deserialize)]
pub struct LedgerSnapshot {
    id: Uuid,
    org_id: Uuid,
    total: i64,
    last_author: Option<Uuid>,
}

impl EventSourced for LedgerAggregate {
    type Key = Uuid;
    type Event = LedgerEvent;
    type Snapshot = LedgerSnapshot;

    const NOUN: NounName = NounName::from_static("ledger");
    const EVENT_VERSION: i32 = EVENT_VERSION;
    const SNAPSHOT_EVERY: i64 = 8;

    fn key(&self) -> Uuid {
        self.0.id
    }

    fn version(&self) -> i64 {
        self.0.version
    }

    fn set_version(&mut self, version: i64) {
        self.0.version = version;
    }

    fn to_snapshot(&self) -> LedgerSnapshot {
        LedgerSnapshot {
            id: self.0.id,
            org_id: self.0.org_id,
            total: self.0.total,
            last_author: self.0.last_author,
        }
    }

    fn from_snapshot(snapshot: LedgerSnapshot) -> Self {
        LedgerAggregate(LedgerState::from_snapshot(
            snapshot.id,
            snapshot.org_id,
            snapshot.total,
            snapshot.last_author,
            0,
        ))
    }

    fn genesis(&self) -> Self {
        LedgerAggregate(LedgerState::from_snapshot(
            self.0.id,
            self.0.org_id,
            0,
            None,
            0,
        ))
    }

    fn apply(&mut self, event: &LedgerEvent) {
        self.0.apply(event);
    }

    fn check_hydrated(&self) -> Result<(), EngineError> {
        self.0.check_hydrated().map_err(EngineError::from)
    }

    fn upcast(version: i32, payload: &serde_json::Value) -> Result<LedgerEvent, EngineError> {
        upcast(version, payload).map_err(EngineError::from)
    }
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
    full_eda::keys::<LedgerAggregate>(pg).await
}
