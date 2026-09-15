use crate::{error::EngineError, nats::Nats, schema::TABLE_MIRROR_WATERMARK};
use sqlx::PgConnection;

#[derive(Clone, Debug)]
pub(super) struct Snapshot {
    pub created: String,
    pub revision: u64,
}
#[derive(sqlx::FromRow)]
pub(super) struct Watermark {
    pub stream_created_at: Option<String>,
    pub revision: i64,
}

pub(super) async fn snapshot(
    nats: &Nats,
    mirror: &str,
    bucket: &str,
) -> Result<Snapshot, EngineError> {
    let stream = nats.bind_stream(&format!("KV_{bucket}")).await?;
    // Always request fresh metadata before any prefix scan, never cached_info().
    let info = stream.get_info().await.map_err(|e| {
        EngineError::Config(format!(
            "mirror {mirror}, bucket {bucket}: cannot read stream metadata: {e}"
        ))
    })?;
    if info.config.max_messages_per_subject != 1 {
        return Err(EngineError::Config(format!(
            "mirror {mirror}, bucket {bucket}: history must be 1 (max_messages_per_subject=1) for safe scan/watch reconciliation; found {}",
            info.config.max_messages_per_subject
        )));
    }
    Ok(Snapshot {
        created: info.created.to_string(),
        revision: info.state.last_sequence,
    })
}

pub(super) async fn read(
    conn: &mut PgConnection,
    mirror: &str,
    bucket: &str,
) -> Result<Option<Watermark>, EngineError> {
    Ok(sqlx::query_as(&format!("SELECT revision, stream_created_at FROM {TABLE_MIRROR_WATERMARK} WHERE mirror=$1 AND bucket=$2"))
        .bind(mirror).bind(bucket).fetch_optional(conn).await?)
}

pub(super) fn compatible(
    mirror: &str,
    bucket: &str,
    held: &Watermark,
    current: &Snapshot,
) -> Result<(), EngineError> {
    let Some(created) = &held.stream_created_at else {
        return Ok(());
    };
    let reason = if created != &current.created {
        Some("stream identity changed")
    } else if held.revision.max(0) as u64 > current.revision {
        Some("stream sequence rollback")
    } else {
        None
    };
    if let Some(reason) = reason {
        // This is the documented operator recovery command, scoped to one mirror
        // and bucket. No failure path deletes or lowers the persisted watermark.
        let mirror_sql = mirror.replace('\'', "''");
        let bucket_sql = bucket.replace('\'', "''");
        return Err(EngineError::Config(format!(
            "mirror {mirror}, bucket {bucket}: {reason}; projections and watermark preserved. After verifying the replacement bucket is authoritative, run with the service database owner: DELETE FROM {TABLE_MIRROR_WATERMARK} WHERE mirror = '{mirror_sql}' AND bucket = '{bucket_sql}'; The next reconciliation adopts the current bucket."
        )));
    }
    Ok(())
}

pub(super) async fn advance(
    conn: &mut PgConnection,
    mirror: &str,
    bucket: &str,
    snapshot: &Snapshot,
) -> Result<(), EngineError> {
    // Identity was checked before projection, in this same transaction. Legacy
    // NULL-identity rows adopt S rather than carrying an untrusted old cursor.
    sqlx::query(&format!("INSERT INTO {TABLE_MIRROR_WATERMARK} (mirror,bucket,revision,stream_created_at) VALUES($1,$2,$3,$4) ON CONFLICT(mirror,bucket) DO UPDATE SET revision=CASE WHEN {TABLE_MIRROR_WATERMARK}.stream_created_at IS NULL THEN EXCLUDED.revision ELSE GREATEST({TABLE_MIRROR_WATERMARK}.revision,EXCLUDED.revision) END, stream_created_at=EXCLUDED.stream_created_at"))
        .bind(mirror).bind(bucket).bind(i64::try_from(snapshot.revision).map_err(|_|EngineError::Config("mirror bucket sequence exceeds PostgreSQL bigint".into()))?).bind(&snapshot.created).execute(conn).await?;
    Ok(())
}
