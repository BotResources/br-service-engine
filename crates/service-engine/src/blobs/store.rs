use std::sync::Arc;

use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::blobs::object::ObjectStore;
use crate::blobs::{BlobRef, DownloadUrl, UploadUrl};
use crate::erase::PersonId;
use crate::error::EngineError;
use crate::schema::TABLE_BLOB;
use crate::time::Timestamp;

pub const REASON_BLOB_BUCKET: &str = "blobs.bucket";
pub const REASON_BLOB_UNCONFIGURED: &str = "blobs.unconfigured";

#[derive(Debug, Clone)]
pub(crate) struct ReferenceRow {
    pub id: Uuid,
    pub object_key: String,
    pub kind: String,
    pub service: String,
    pub content_type: String,
    pub file_name: String,
    pub owner: Option<Uuid>,
}

pub(crate) enum BlobRowOp {
    Insert(ReferenceRow),
    Orphan(BlobRef, Timestamp),
}

pub(crate) async fn insert_reference(
    conn: &mut PgConnection,
    row: &ReferenceRow,
) -> Result<(), EngineError> {
    sqlx::query(&format!(
        "INSERT INTO {TABLE_BLOB} \
           (id, object_key, kind, service, content_type, file_name, owner_id, state) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending')"
    ))
    .bind(row.id)
    .bind(&row.object_key)
    .bind(&row.kind)
    .bind(&row.service)
    .bind(&row.content_type)
    .bind(&row.file_name)
    .bind(row.owner)
    .execute(conn)
    .await?;
    Ok(())
}

pub(crate) async fn orphan_reference(
    conn: &mut PgConnection,
    reference: BlobRef,
    at: Timestamp,
) -> Result<(), EngineError> {
    sqlx::query(&format!(
        "UPDATE {TABLE_BLOB} SET state = 'orphaned', detached_at = $2 \
         WHERE id = $1 AND state <> 'orphaned'"
    ))
    .bind(reference.as_uuid())
    .bind(at.as_datetime())
    .execute(conn)
    .await?;
    Ok(())
}

#[derive(Clone)]
pub(crate) struct BlobStore {
    object: Arc<ObjectStore>,
    service: Arc<str>,
}

impl BlobStore {
    pub(crate) fn new(object: Arc<ObjectStore>, service: &str) -> Self {
        Self {
            object,
            service: Arc::from(service),
        }
    }

    pub(crate) fn service(&self) -> &str {
        &self.service
    }

    pub(crate) fn object(&self) -> &ObjectStore {
        &self.object
    }

    pub(crate) fn presign_upload(
        &self,
        object_key: &str,
        max_bytes: u64,
    ) -> Result<UploadUrl, EngineError> {
        self.object.presign_upload(object_key, max_bytes)
    }

    pub(crate) async fn download_url(
        &self,
        pool: &PgPool,
        reference: BlobRef,
    ) -> Result<Option<DownloadUrl>, EngineError> {
        let row: Option<(String, String)> = sqlx::query_as(&format!(
            "SELECT object_key, state FROM {TABLE_BLOB} WHERE id = $1"
        ))
        .bind(reference.as_uuid())
        .fetch_optional(pool)
        .await?;
        let Some((object_key, state)) = row else {
            return Ok(None);
        };
        match state.as_str() {
            "uploaded" => Ok(Some(self.object.presign_download(&object_key))),
            "pending" => match self.object.head_size(&object_key).await? {
                Some(_) => Ok(Some(self.object.presign_download(&object_key))),
                None => Ok(None),
            },
            _ => Ok(None),
        }
    }

    pub(crate) async fn purge_person(
        &self,
        pool: &PgPool,
        person: PersonId,
    ) -> Result<u64, EngineError> {
        let rows = sqlx::query(&format!(
            "SELECT id, object_key FROM {TABLE_BLOB} WHERE owner_id = $1"
        ))
        .bind(person.as_uuid())
        .fetch_all(pool)
        .await?;
        let mut purged = 0;
        for row in &rows {
            let id: Uuid = row.get("id");
            let object_key: String = row.get("object_key");
            self.object.delete_object(&object_key).await?;
            sqlx::query(&format!("DELETE FROM {TABLE_BLOB} WHERE id = $1"))
                .bind(id)
                .execute(pool)
                .await?;
            purged += 1;
        }
        Ok(purged)
    }
}

impl std::fmt::Debug for BlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobStore")
            .field("service", &self.service)
            .finish_non_exhaustive()
    }
}
