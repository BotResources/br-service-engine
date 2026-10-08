use std::time::Duration;

use sqlx::PgConnection;

use crate::advisory;
use crate::error::EngineError;
use crate::name::{MirrorName, PodId};
use crate::schema::TABLE_LEADER_SLOT;

#[derive(Clone, Debug)]
pub struct MirrorLeader {
    pod: PodId,
    lease: Duration,
    beat: Duration,
}

impl MirrorLeader {
    pub fn new(pod: PodId, lease: Duration, beat: Duration) -> Self {
        Self { pod, lease, beat }
    }
}

pub(super) struct MirrorGate {
    pub(super) pod: PodId,
    pub(super) lease: Duration,
    pub(super) beat: Duration,
    pub(super) slot_name: String,
    pub(super) advisory_key: i64,
}

impl MirrorGate {
    pub(super) fn new(name: &MirrorName, leader: MirrorLeader) -> Self {
        Self {
            pod: leader.pod,
            lease: leader.lease,
            beat: leader.beat,
            slot_name: format!("mirror:{}", name.as_str()),
            advisory_key: advisory::lock_id(
                advisory::LEADER_SLOT,
                &[b"mirror", name.as_str().as_bytes()],
            ),
        }
    }
}

/// Whether this pod may lead: the lease is its own, expired, or never taken. A
/// plain read, so a standby answers "not me" without touching the advisory lock
/// or the lease row while the leader projects.
pub(super) async fn lease_open_to(
    conn: &mut PgConnection,
    slot_name: &str,
    pod: &str,
) -> Result<bool, EngineError> {
    let open: bool = sqlx::query_scalar(&format!(
        "SELECT NOT EXISTS ( \
           SELECT 1 FROM {TABLE_LEADER_SLOT} \
            WHERE name = $1 AND slot = 'epoch'::timestamptz \
              AND pod <> $2 AND lease_until > now())"
    ))
    .bind(slot_name)
    .bind(pod)
    .fetch_one(conn)
    .await?;
    Ok(open)
}

/// Waits for the mirror's projection lock. Only a pod the lease is open to asks
/// for it, so the wait is bounded by one projection of the other contender.
pub(super) async fn advisory_xact_lock(
    conn: &mut PgConnection,
    key: i64,
) -> Result<(), EngineError> {
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(conn)
        .await?;
    Ok(())
}

pub(super) async fn hold_lease(
    conn: &mut PgConnection,
    slot_name: &str,
    pod: &str,
    lease: Duration,
) -> Result<bool, EngineError> {
    let held: Option<String> = sqlx::query_scalar(&format!(
        "INSERT INTO {TABLE_LEADER_SLOT} (name, slot, pod, lease_until, completed_at) \
           VALUES ($1, 'epoch'::timestamptz, $2, now() + make_interval(secs => $3), NULL) \
         ON CONFLICT (name, slot) DO UPDATE \
           SET pod = EXCLUDED.pod, lease_until = EXCLUDED.lease_until \
           WHERE {TABLE_LEADER_SLOT}.pod = EXCLUDED.pod \
              OR {TABLE_LEADER_SLOT}.lease_until <= now() \
         RETURNING pod"
    ))
    .bind(slot_name)
    .bind(pod)
    .bind(lease.as_secs_f64())
    .fetch_optional(conn)
    .await?;
    Ok(held.as_deref() == Some(pod))
}
