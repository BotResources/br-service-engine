use std::time::Duration;

use sqlx::{PgConnection, Row};

use crate::error::EngineError;
use crate::name::PodId;
use crate::schema::TABLE_LEADER_SLOT;
use crate::time::Timestamp;

mod slot;

pub use slot::{Lease, SlotKind, SlotName, advisory_key};

pub const DEFAULT_SLOT_PERIOD: Duration = crate::config::DEFAULT_BEAT;

pub async fn try_advisory_xact_lock(
    conn: &mut PgConnection,
    key: i64,
) -> Result<bool, EngineError> {
    let held: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(key)
        .fetch_one(conn)
        .await?;
    Ok(held)
}

pub async fn claim_current_slot(
    conn: &mut PgConnection,
    name: SlotName,
    period: Duration,
    pod: &PodId,
    lease: Duration,
) -> Result<Option<Lease>, EngineError> {
    if period.is_zero() {
        return Err(EngineError::Config("a slot period must be non-zero".into()));
    }
    refuse_zero_lease(lease)?;
    let sql = claim_sql("date_bin(make_interval(secs => $2), now(), 'epoch'::timestamptz)");
    let row = sqlx::query(&sql)
        .bind(name.qualified())
        .bind(period.as_secs_f64())
        .bind(pod.as_str())
        .bind(lease.as_secs_f64())
        .fetch_optional(conn)
        .await?;
    Ok(row.map(|row| held(name, pod, &row)))
}

pub async fn claim_slot_at(
    conn: &mut PgConnection,
    name: SlotName,
    slot: Timestamp,
    pod: &PodId,
    lease: Duration,
) -> Result<Option<Lease>, EngineError> {
    refuse_zero_lease(lease)?;
    let sql = claim_sql("$2::timestamptz");
    let row = sqlx::query(&sql)
        .bind(name.qualified())
        .bind(slot)
        .bind(pod.as_str())
        .bind(lease.as_secs_f64())
        .fetch_optional(conn)
        .await?;
    Ok(row.map(|row| held(name, pod, &row)))
}

pub async fn renew_slot(
    conn: &mut PgConnection,
    lease: &mut Lease,
    duration: Duration,
) -> Result<bool, EngineError> {
    refuse_zero_lease(duration)?;
    let sql = format!(
        "UPDATE {TABLE_LEADER_SLOT} \
            SET lease_until = now() + make_interval(secs => $4) \
          WHERE name = $1 AND slot = $2 AND pod = $3 AND completed_at IS NULL \
         RETURNING lease_until"
    );
    let row = sqlx::query(&sql)
        .bind(lease.qualified_name())
        .bind(lease.slot)
        .bind(lease.pod.as_str())
        .bind(duration.as_secs_f64())
        .fetch_optional(conn)
        .await?;
    match row {
        Some(row) => {
            lease.lease_until = row.get("lease_until");
            Ok(true)
        }
        None => Ok(false),
    }
}

pub async fn complete_slot(conn: &mut PgConnection, lease: &Lease) -> Result<bool, EngineError> {
    let sql = format!(
        "UPDATE {TABLE_LEADER_SLOT} \
            SET completed_at = now() \
          WHERE name = $1 AND slot = $2 AND pod = $3 AND completed_at IS NULL \
         RETURNING completed_at"
    );
    let row = sqlx::query(&sql)
        .bind(lease.qualified_name())
        .bind(lease.slot)
        .bind(lease.pod.as_str())
        .fetch_optional(conn)
        .await?;
    Ok(row.is_some())
}

pub async fn sweep_completed_slots(
    conn: &mut PgConnection,
    older_than: Duration,
) -> Result<u64, EngineError> {
    let sql = format!(
        "DELETE FROM {TABLE_LEADER_SLOT} \
          WHERE completed_at IS NOT NULL \
            AND completed_at < now() - make_interval(secs => $1)"
    );
    let done = sqlx::query(&sql)
        .bind(older_than.as_secs_f64())
        .execute(conn)
        .await?;
    Ok(done.rows_affected())
}

pub async fn sweep_abandoned_slots(
    conn: &mut PgConnection,
    older_than: Duration,
) -> Result<u64, EngineError> {
    let sql = format!(
        "DELETE FROM {TABLE_LEADER_SLOT} \
          WHERE completed_at IS NULL \
            AND lease_until < now() - make_interval(secs => $1)"
    );
    let done = sqlx::query(&sql)
        .bind(older_than.as_secs_f64())
        .execute(conn)
        .await?;
    Ok(done.rows_affected())
}

fn claim_sql(slot: &str) -> String {
    format!(
        "INSERT INTO {TABLE_LEADER_SLOT} AS held (name, slot, pod, lease_until, completed_at) \
         SELECT $1, {slot}, $3, now() + make_interval(secs => $4), NULL \
         ON CONFLICT (name, slot) DO UPDATE \
            SET pod = EXCLUDED.pod, lease_until = EXCLUDED.lease_until \
          WHERE held.completed_at IS NULL AND held.lease_until <= now() \
         RETURNING slot, lease_until"
    )
}

fn held(name: SlotName, pod: &PodId, row: &sqlx::postgres::PgRow) -> Lease {
    Lease {
        name,
        slot: row.get("slot"),
        pod: pod.clone(),
        lease_until: row.get("lease_until"),
    }
}

fn refuse_zero_lease(lease: Duration) -> Result<(), EngineError> {
    if lease.is_zero() {
        return Err(EngineError::Config(
            "a slot lease must be non-zero, or it expires before its holder can run".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lease_that_expires_the_instant_it_is_taken_is_refused() {
        assert!(refuse_zero_lease(Duration::ZERO).is_err());
        assert!(refuse_zero_lease(Duration::from_millis(1)).is_ok());
    }
}
