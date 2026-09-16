use sqlx::PgConnection;

use crate::error::EngineError;
use crate::nats::Nats;
use crate::schema::TABLE_MIRROR_WATERMARK;

/// What a bucket looked like at the instant a full read started: the stream's
/// creation identity, and the last sequence `S` the read is a snapshot of.
#[derive(Clone, Debug)]
pub(super) struct Snapshot {
    pub created: String,
    pub revision: u64,
}

/// What the mirror has committed for a bucket. `stream_created_at` is NULL on
/// rows written before the identity column existed.
#[derive(sqlx::FromRow)]
pub(super) struct Watermark {
    pub stream_created_at: Option<String>,
    pub revision: i64,
}

impl Watermark {
    pub(super) fn revision(&self) -> u64 {
        self.revision.max(0) as u64
    }
}

/// How a snapshot is written. A full read *adopts* what it just read, so a
/// replaced or restored bucket takes the sequence it actually has. A watch
/// event only *advances*, since it observes one revision of the stream a read
/// already adopted.
#[derive(Clone, Copy)]
pub(super) enum Mark {
    Adopt,
    Advance,
}

/// Fresh stream metadata, never `cached_info()`: the snapshot boundary is only
/// sound when it is read at the instant the scan starts.
pub(super) async fn snapshot(
    nats: &Nats,
    mirror: &str,
    bucket: &str,
) -> Result<Snapshot, EngineError> {
    let stream = nats.bind_stream(&format!("KV_{bucket}")).await?;
    let info = stream.get_info().await.map_err(|error| {
        EngineError::Config(format!(
            "mirror {mirror}, bucket {bucket}: cannot read stream metadata: {error}"
        ))
    })?;
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
    Ok(sqlx::query_as(&format!(
        "SELECT revision, stream_created_at FROM {TABLE_MIRROR_WATERMARK} \
         WHERE mirror = $1 AND bucket = $2"
    ))
    .bind(mirror)
    .bind(bucket)
    .fetch_optional(conn)
    .await?)
}

pub(super) async fn write(
    conn: &mut PgConnection,
    mirror: &str,
    bucket: &str,
    snapshot: &Snapshot,
    mark: Mark,
) -> Result<(), EngineError> {
    // Adoption takes the read sequence as it is — a replaced or restored bucket
    // is a new cursor, not a regression. An advance stays monotonic inside one
    // identity, and adopts when the identity under it changed.
    let revision = match mark {
        Mark::Adopt => "EXCLUDED.revision".to_string(),
        Mark::Advance => format!(
            "CASE WHEN {TABLE_MIRROR_WATERMARK}.stream_created_at \
                       IS DISTINCT FROM EXCLUDED.stream_created_at \
                  THEN EXCLUDED.revision \
                  ELSE GREATEST({TABLE_MIRROR_WATERMARK}.revision, EXCLUDED.revision) END"
        ),
    };
    sqlx::query(&format!(
        "INSERT INTO {TABLE_MIRROR_WATERMARK} (mirror, bucket, revision, stream_created_at) \
           VALUES ($1, $2, $3, $4) \
         ON CONFLICT (mirror, bucket) DO UPDATE \
           SET revision = {revision}, stream_created_at = EXCLUDED.stream_created_at"
    ))
    .bind(mirror)
    .bind(bucket)
    .bind(i64::try_from(snapshot.revision).map_err(|_| {
        EngineError::Config(format!(
            "mirror {mirror}, bucket {bucket}: stream sequence exceeds a PostgreSQL bigint"
        ))
    })?)
    .bind(&snapshot.created)
    .execute(conn)
    .await?;
    Ok(())
}
