use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::accumulator::guard;
use crate::blobs::BlobRef;
use crate::engine::BlobReader;
use crate::erase::person::PersonId;
use crate::error::EngineError;
use crate::name::AccumulatorName;
use crate::nats::KvKey;
use crate::presence::PresenceHandle;
use crate::principal::Principal;
use crate::schema::{TABLE_ACCUMULATOR_CHUNK, TABLE_ACCUMULATOR_SEAL, TABLE_PERSON_ERASURE};
use crate::time::{self, Timestamp};
use crate::wire::KeyBytes;

const DRAIN_BATCH: i64 = 128;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(crate) struct PurgeManifest {
    #[serde(default)]
    pub streams: Vec<PurgeStream>,
    #[serde(default)]
    pub presence: Vec<String>,
    #[serde(default)]
    pub blobs: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PurgeStream {
    pub accumulator: String,
    pub key: serde_json::Value,
}

impl PurgeManifest {
    pub(crate) fn to_value(&self) -> Result<serde_json::Value, EngineError> {
        serde_json::to_value(self).map_err(|source| EngineError::Encode {
            what: "person erasure manifest",
            source,
        })
    }

    fn from_value(value: serde_json::Value) -> Result<Self, EngineError> {
        serde_json::from_value(value).map_err(|source| EngineError::Decode {
            what: "person erasure manifest",
            source,
        })
    }
}

pub(crate) async fn complete_one<P: Principal>(
    pg: &PgPool,
    presence: &PresenceHandle<P>,
    blobs: &BlobReader,
    person: PersonId,
) -> Result<u64, EngineError> {
    let Some(row) = sqlx::query(&format!(
        "SELECT manifest, purged_at FROM {TABLE_PERSON_ERASURE} WHERE person_id = $1"
    ))
    .bind(person.as_uuid())
    .fetch_optional(pg)
    .await?
    else {
        return Ok(0);
    };
    if row.get::<Option<Timestamp>, _>("purged_at").is_some() {
        return Ok(0);
    }
    let manifest = PurgeManifest::from_value(row.get("manifest"))?;
    let purged = purge_manifest(pg, presence, blobs, person, &manifest).await?;
    mark_purged(pg, person, time::now()).await?;
    Ok(purged)
}

pub(crate) async fn drain_pending<P: Principal>(
    pg: &PgPool,
    presence: &PresenceHandle<P>,
    blobs: &BlobReader,
) -> Result<u64, EngineError> {
    let rows = sqlx::query(&format!(
        "SELECT person_id, manifest FROM {TABLE_PERSON_ERASURE} \
         WHERE purged_at IS NULL ORDER BY erased_at LIMIT $1"
    ))
    .bind(DRAIN_BATCH)
    .fetch_all(pg)
    .await?;
    let mut completed = 0;
    for row in rows {
        let person = PersonId(row.get::<Uuid, _>("person_id"));
        let manifest = PurgeManifest::from_value(row.get("manifest"))?;
        purge_manifest(pg, presence, blobs, person, &manifest).await?;
        mark_purged(pg, person, time::now()).await?;
        completed += 1;
    }
    Ok(completed)
}

async fn purge_manifest<P: Principal>(
    pg: &PgPool,
    presence: &PresenceHandle<P>,
    blobs: &BlobReader,
    person: PersonId,
    manifest: &PurgeManifest,
) -> Result<u64, EngineError> {
    purge_streams(pg, &manifest.streams, time::now()).await?;
    let presence_keys = manifest
        .presence
        .iter()
        .map(|raw| KvKey::new(raw.clone()).map_err(|error| EngineError::Service(Box::new(error))))
        .collect::<Result<Vec<_>, _>>()?;
    presence.purge(&presence_keys).await?;
    let blob_refs: Vec<BlobRef> = manifest.blobs.iter().copied().map(BlobRef).collect();
    let named = blobs.purge_references(&blob_refs).await?;
    let owned = blobs.purge_person(person).await?;
    Ok(named + owned)
}

async fn purge_streams(
    pg: &PgPool,
    streams: &[PurgeStream],
    at: Timestamp,
) -> Result<(), EngineError> {
    for stream in streams {
        let stream_key = (
            AccumulatorName::new(stream.accumulator.clone())?,
            KeyBytes::encode(&stream.key)?,
        );
        let mut tx = pg.begin().await?;
        guard::hold(&mut tx, std::slice::from_ref(&stream_key)).await?;
        sqlx::query(&format!(
            "INSERT INTO {TABLE_ACCUMULATOR_SEAL} (accumulator, key, high_water, sealed_at, purged_at) \
             SELECT $1, $2::jsonb, COALESCE(MAX(seq) + 1, 0), $3, NULL \
             FROM {TABLE_ACCUMULATOR_CHUNK} WHERE accumulator = $1 AND key = $2::jsonb \
             ON CONFLICT (accumulator, key) DO UPDATE \
             SET high_water = GREATEST(EXCLUDED.high_water, accumulator_seal.high_water), \
                 purged_at = NULL"
        ))
        .bind(&stream.accumulator)
        .bind(&stream.key)
        .bind(at)
        .execute(&mut *tx)
        .await?;
        sqlx::query(&format!(
            "DELETE FROM {TABLE_ACCUMULATOR_CHUNK} WHERE accumulator = $1 AND key = $2::jsonb"
        ))
        .bind(&stream.accumulator)
        .bind(&stream.key)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
    }
    Ok(())
}

async fn mark_purged(pg: &PgPool, person: PersonId, at: Timestamp) -> Result<(), EngineError> {
    sqlx::query(&format!(
        "UPDATE {TABLE_PERSON_ERASURE} SET purged_at = $2 \
         WHERE person_id = $1 AND purged_at IS NULL"
    ))
    .bind(person.as_uuid())
    .bind(at)
    .execute(pg)
    .await?;
    Ok(())
}
