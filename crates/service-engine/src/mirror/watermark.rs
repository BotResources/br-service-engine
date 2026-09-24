use sqlx::PgConnection;

use crate::error::EngineError;
use crate::nats::Nats;
use crate::schema::TABLE_MIRROR_WATERMARK;

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

impl Watermark {
    pub(super) fn revision(&self) -> u64 {
        self.revision.max(0) as u64
    }
}

#[derive(Clone, Copy)]
pub(super) enum Mark {
    Adopt,
    Advance,
}

pub(super) async fn snapshot(
    nats: &Nats,
    mirror: &str,
    bucket: &str,
) -> Result<Snapshot, EngineError> {
    let stream = nats.bind_stream(&format!("KV_{bucket}")).await?;
    let info = stream.get_info().await.map_err(|error| {
        EngineError::Config(format!(
            "mirror {mirror}, bucket {bucket}: cannot read stream metadata: {}",
            crate::chain::describe(&error)
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
