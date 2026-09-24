use conformance_service_engine::infra::TestDb;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::name::NounName;
use service_engine::persistence::Persistence;
use service_engine::{EventSourced, FullEda};
use sqlx::PgConnection;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Tally {
    id: Uuid,
    total: i64,
    version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Added {
    amount: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TallySnapshot {
    id: Uuid,
    total: i64,
}

impl EventSourced for Tally {
    type Key = Uuid;
    type Event = Added;
    type Snapshot = TallySnapshot;

    const NOUN: NounName = NounName::from_static("s250_tally");
    const EVENT_VERSION: i32 = 1;
    const SNAPSHOT_EVERY: i64 = 3;

    fn key(&self) -> Uuid {
        self.id
    }

    fn version(&self) -> i64 {
        self.version
    }

    fn set_version(&mut self, version: i64) {
        self.version = version;
    }

    fn to_snapshot(&self) -> TallySnapshot {
        TallySnapshot {
            id: self.id,
            total: self.total,
        }
    }

    fn from_snapshot(snapshot: TallySnapshot) -> Self {
        Self {
            id: snapshot.id,
            total: snapshot.total,
            version: 0,
        }
    }

    fn genesis(&self) -> Self {
        Self {
            id: self.id,
            total: 0,
            version: 0,
        }
    }

    fn apply(&mut self, event: &Added) {
        self.total += event.amount;
    }

    fn check_hydrated(&self) -> Result<(), EngineError> {
        Ok(())
    }

    fn upcast(_version: i32, payload: &serde_json::Value) -> Result<Added, EngineError> {
        serde_json::from_value(payload.clone()).map_err(|source| EngineError::Decode {
            what: "s250 tally event",
            source,
        })
    }
}

type TallyStore = FullEda<Tally>;

async fn tally_with(conn: &mut PgConnection, amounts: &[i64]) -> Uuid {
    let id = Uuid::now_v7();
    let mut tally = Tally {
        id,
        total: 0,
        version: 0,
    };
    TallyStore::create(conn, &tally, &[])
        .await
        .expect("create the tally");
    for amount in amounts {
        tally.total += amount;
        tally.version += 1;
        TallyStore::save(conn, &tally, &[Added { amount: *amount }])
            .await
            .expect("append one event");
    }
    id
}

#[tokio::test]
async fn s250_a_batched_read_hydrates_every_key_to_the_state_a_single_load_reaches() {
    let db = TestDb::fresh().await;
    let mut conn = db.app_pool().acquire().await.expect("a connection");

    let fresh = tally_with(&mut conn, &[]).await;
    let on_cadence = tally_with(&mut conn, &[1, 2, 3]).await;
    let past_snapshot = tally_with(&mut conn, &[5, 7, 11, 13, 17]).await;
    let absent = Uuid::now_v7();
    let keys = [fresh, on_cadence, absent, past_snapshot];

    let mut batched: Vec<(Uuid, i64, i64)> = TallyStore::read_many(&mut conn, &keys)
        .await
        .expect("the batched read succeeds")
        .into_iter()
        .map(|(key, tally)| {
            assert_eq!(key, tally.id, "each aggregate comes back under its own key");
            (key, tally.total, tally.version)
        })
        .collect();
    batched.sort();
    let mut expected = vec![(fresh, 0, 0), (on_cadence, 6, 3), (past_snapshot, 53, 5)];
    expected.sort();
    assert_eq!(
        batched, expected,
        "one batched read hydrates every present key to its own state, replays the events \
         appended after the last snapshot, and omits the absent key"
    );

    let single = TallyStore::load(&mut conn, &past_snapshot)
        .await
        .expect("the single load succeeds")
        .expect("the tally exists");
    assert_eq!(
        (single.total, single.version),
        (53, 5),
        "load, derived from read_many, reaches the same state"
    );

    drop(conn);
    db.cleanup().await;
}
