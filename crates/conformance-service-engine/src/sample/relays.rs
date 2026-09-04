use std::time::Duration;

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::RelayError;
use service_engine::housekeeping::leader::SlotName;
use service_engine::name::RelayName;
use service_engine::relay::{Claim, Discipline, Drained, Relay};
use service_engine::relays::kv::{KvChange, KvSource, Versioned};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use br_util_nats_fabric::KvKey;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SampleRoster {
    pub label: String,
    pub version: u64,
}

impl Versioned for SampleRoster {
    fn version(&self) -> u64 {
        self.version
    }
}

pub struct RowClaimSampleRelay {
    name: RelayName,
    hold: Duration,
}

impl RowClaimSampleRelay {
    pub fn new(name: RelayName, hold: Duration) -> Self {
        Self { name, hold }
    }
}

impl Relay for RowClaimSampleRelay {
    fn name(&self) -> RelayName {
        self.name.clone()
    }

    fn drain<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        claim: &'a Claim,
    ) -> BoxFuture<'a, Result<Drained, RelayError>> {
        Box::pin(async move {
            let batch = claim.batch().max(1);
            let sql = format!(
                "SELECT id FROM sample_relay_row WHERE claimed_at IS NULL \
                 ORDER BY id LIMIT $1 {}",
                claim.for_update_skip_locked()
            );
            let ids: Vec<Uuid> = sqlx::query_scalar(&sql)
                .bind(i64::try_from(batch).unwrap_or(i64::MAX))
                .fetch_all(&mut *conn)
                .await?;
            tokio::time::sleep(self.hold).await;
            for id in &ids {
                sqlx::query(
                    "UPDATE sample_relay_row SET claimed_by = $2, claimed_at = now() WHERE id = $1",
                )
                .bind(id)
                .bind(claim.pod().as_str())
                .execute(&mut *conn)
                .await?;
                sqlx::query(
                    "INSERT INTO sample_relay_claim (id, row_id, claimed_by) VALUES ($1, $2, $3)",
                )
                .bind(Uuid::now_v7())
                .bind(id)
                .bind(claim.pod().as_str())
                .execute(&mut *conn)
                .await?;
            }
            Ok(Drained::rows(ids.len(), ids.len() >= batch))
        })
    }
}

pub struct LeaderRunSampleRelay {
    name: RelayName,
}

impl LeaderRunSampleRelay {
    pub fn new(name: RelayName) -> Self {
        Self { name }
    }
}

impl Relay for LeaderRunSampleRelay {
    fn name(&self) -> RelayName {
        self.name.clone()
    }

    fn discipline(&self) -> Discipline {
        Discipline::Leader
    }

    fn drain<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        claim: &'a Claim,
    ) -> BoxFuture<'a, Result<Drained, RelayError>> {
        Box::pin(async move {
            let slot: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
                "SELECT slot FROM service_engine.leader_slot \
                 WHERE name = $1 AND pod = $2 AND completed_at IS NULL \
                 ORDER BY slot DESC LIMIT 1",
            )
            .bind(SlotName::Relay(self.name.clone()).qualified())
            .bind(claim.pod().as_str())
            .fetch_optional(&mut *conn)
            .await?;
            let slot = slot.ok_or_else(|| {
                RelayError::Publish(
                    "a Leader relay ran without the engine holding its slot".to_string(),
                )
            })?;
            sqlx::query("INSERT INTO sample_leader_run (id, slot, pod) VALUES ($1, $2, $3)")
                .bind(Uuid::now_v7())
                .bind(slot)
                .bind(claim.pod().as_str())
                .execute(&mut *conn)
                .await?;
            Ok(Drained::rows(1, false))
        })
    }
}

pub struct BusySampleRelay {
    name: RelayName,
    drains: std::sync::atomic::AtomicUsize,
}

impl BusySampleRelay {
    pub fn new(name: RelayName) -> Self {
        Self {
            name,
            drains: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn drains(&self) -> usize {
        self.drains.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Relay for BusySampleRelay {
    fn name(&self) -> RelayName {
        self.name.clone()
    }

    fn drain<'a>(
        &'a self,
        _conn: &'a mut PgConnection,
        claim: &'a Claim,
    ) -> BoxFuture<'a, Result<Drained, RelayError>> {
        Box::pin(async move {
            self.drains
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Drained::rows(claim.batch(), true))
        })
    }
}

pub struct FailingSampleRelay {
    name: RelayName,
    attempts: std::sync::atomic::AtomicUsize,
}

impl FailingSampleRelay {
    pub fn new(name: RelayName) -> Self {
        Self {
            name,
            attempts: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn attempts(&self) -> usize {
        self.attempts.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Relay for FailingSampleRelay {
    fn name(&self) -> RelayName {
        self.name.clone()
    }

    fn drain<'a>(
        &'a self,
        _conn: &'a mut PgConnection,
        _claim: &'a Claim,
    ) -> BoxFuture<'a, Result<Drained, RelayError>> {
        Box::pin(async move {
            self.attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(RelayError::Publish(
                "the sample failing relay never publishes anything".to_string(),
            ))
        })
    }
}

pub struct SampleKvSource {
    hold: Duration,
}

impl SampleKvSource {
    pub fn new(hold: Duration) -> Self {
        Self { hold }
    }
}

impl KvSource<SampleRoster> for SampleKvSource {
    fn pending<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        batch: usize,
    ) -> BoxFuture<'a, Result<Vec<KvChange<SampleRoster>>, RelayError>> {
        Box::pin(async move {
            let rows = sqlx::query(
                "SELECT key, version, label FROM sample_kv_pending \
                 WHERE applied_at IS NULL ORDER BY key LIMIT $1 FOR UPDATE",
            )
            .bind(i64::try_from(batch).unwrap_or(i64::MAX))
            .fetch_all(&mut *conn)
            .await?;
            tokio::time::sleep(self.hold).await;
            rows.into_iter()
                .map(|row| {
                    let key = KvKey::new(row.get::<String, _>("key"))
                        .map_err(|e| RelayError::Publish(format!("sample kv key: {e}")))?;
                    let version = u64::try_from(row.get::<i64, _>("version")).unwrap_or_default();
                    match row.get::<Option<String>, _>("label") {
                        Some(label) => Ok(KvChange::put(key, SampleRoster { label, version })),
                        None => Ok(KvChange::retract(key, version)),
                    }
                })
                .collect()
        })
    }

    fn applied<'a>(
        &'a self,
        conn: &'a mut PgConnection,
        applied: &'a [KvChange<SampleRoster>],
    ) -> BoxFuture<'a, Result<(), RelayError>> {
        Box::pin(async move {
            let names: Vec<String> = applied.iter().map(|c| c.key.as_str().to_string()).collect();
            let versions: Vec<i64> = applied
                .iter()
                .map(|c| i64::try_from(c.version).unwrap_or(i64::MAX))
                .collect();
            sqlx::query(
                "UPDATE sample_kv_pending AS pending SET applied_at = now() \
                 FROM unnest($1::text[], $2::bigint[]) AS drained(key, version) \
                 WHERE pending.key = drained.key \
                   AND pending.version = drained.version \
                   AND pending.applied_at IS NULL",
            )
            .bind(&names)
            .bind(&versions)
            .execute(&mut *conn)
            .await?;
            Ok(())
        })
    }
}
