use std::collections::BTreeSet;
use std::sync::Arc;

use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::blobs::expect::{Sha256Digest, UploadExpectation};
use crate::blobs::head::{BlobHead, BlobState};
use crate::blobs::object::ObjectStore;
use crate::blobs::{BlobRef, Disposition, DownloadUrl, UploadUrl};
use crate::erase::PersonId;
use crate::error::EngineError;
use crate::schema::TABLE_BLOB;
use crate::time::Timestamp;

pub const REASON_BLOB_BUCKET: &str = "blobs.bucket";
pub const REASON_BLOB_UNCONFIGURED: &str = "blobs.unconfigured";
pub const REASON_BLOB_POLICY: &str = "blobs.policy";

type HeadRow = (String, String, Option<i64>, Option<Vec<u8>>);

#[derive(Debug, Clone)]
pub(crate) struct ReferenceRow {
    pub id: Uuid,
    pub object_key: String,
    pub kind: String,
    pub service: String,
    pub content_type: String,
    pub file_name: String,
    pub owner: Option<Uuid>,
    pub expected_size: Option<i64>,
    pub expected_sha256: Option<[u8; 32]>,
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
           (id, object_key, kind, service, content_type, file_name, owner_id, state, \
            expected_size, expected_sha256) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $9)"
    ))
    .bind(row.id)
    .bind(&row.object_key)
    .bind(&row.kind)
    .bind(&row.service)
    .bind(&row.content_type)
    .bind(&row.file_name)
    .bind(row.owner)
    .bind(row.expected_size)
    .bind(row.expected_sha256.map(|bytes| bytes.to_vec()))
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

pub(crate) fn pending_is_downloadable(has_policy: bool, has_expectation: bool) -> bool {
    !has_policy && !has_expectation
}

#[derive(Clone)]
pub(crate) struct BlobStore {
    object: Arc<ObjectStore>,
    service: Arc<str>,
    policy_kinds: Arc<BTreeSet<&'static str>>,
}

impl BlobStore {
    pub(crate) fn new(
        object: Arc<ObjectStore>,
        service: &str,
        policy_kinds: Arc<BTreeSet<&'static str>>,
    ) -> Self {
        Self {
            object,
            service: Arc::from(service),
            policy_kinds,
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
        content_type: &str,
        expect: Option<(u64, &Sha256Digest)>,
    ) -> Result<UploadUrl, EngineError> {
        self.object
            .presign_upload(object_key, max_bytes, content_type, expect)
    }

    pub(crate) async fn download_url(
        &self,
        pool: &PgPool,
        reference: BlobRef,
        disposition: Disposition,
    ) -> Result<Option<DownloadUrl>, EngineError> {
        let row: Option<(String, String, String, String, String, Option<i64>)> =
            sqlx::query_as(&format!(
                "SELECT object_key, state, content_type, file_name, kind, expected_size \
                 FROM {TABLE_BLOB} WHERE id = $1"
            ))
            .bind(reference.as_uuid())
            .fetch_optional(pool)
            .await?;
        let Some((object_key, state, content_type, file_name, kind, expected_size)) = row else {
            return Ok(None);
        };
        let presign = || {
            self.object
                .presign_download(&object_key, &content_type, &file_name, disposition)
        };
        match state.as_str() {
            "uploaded" => Ok(Some(presign())),
            "pending" => {
                let has_policy = self.policy_kinds.contains(kind.as_str());
                let has_expectation = expected_size.is_some();
                if !pending_is_downloadable(has_policy, has_expectation) {
                    return Ok(None);
                }
                match self.object.head(&object_key).await? {
                    Some(_) => Ok(Some(presign())),
                    None => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    pub(crate) async fn head(
        &self,
        pool: &PgPool,
        reference: BlobRef,
    ) -> Result<Option<BlobHead>, EngineError> {
        let row: Option<HeadRow> = sqlx::query_as(&format!(
            "SELECT object_key, state, expected_size, expected_sha256 \
             FROM {TABLE_BLOB} WHERE id = $1"
        ))
        .bind(reference.as_uuid())
        .fetch_optional(pool)
        .await?;
        let Some((object_key, state, expected_size, expected_sha256)) = row else {
            return Ok(None);
        };
        let Some(object) = self.object.head(&object_key).await? else {
            return Ok(None);
        };
        let expected = match (expected_size, expected_sha256) {
            (Some(size), Some(bytes)) => {
                let array: [u8; 32] = bytes.try_into().map_err(|_| {
                    EngineError::Blob(format!(
                        "the recorded expected_sha256 of blob {} is not 32 bytes",
                        reference.as_uuid()
                    ))
                })?;
                Some(UploadExpectation::new(
                    size as u64,
                    Sha256Digest::from_bytes(array),
                ))
            }
            _ => None,
        };
        Ok(Some(BlobHead {
            state: BlobState::from_row(&state),
            expected,
            size: object.size,
            etag: object.etag,
            sha256: object.sha256,
        }))
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

    pub(crate) async fn purge_references(
        &self,
        pool: &PgPool,
        references: &[BlobRef],
    ) -> Result<u64, EngineError> {
        let mut purged = 0;
        for reference in references {
            let object_key: Option<String> = sqlx::query_scalar(&format!(
                "SELECT object_key FROM {TABLE_BLOB} WHERE id = $1"
            ))
            .bind(reference.as_uuid())
            .fetch_optional(pool)
            .await?;
            let Some(object_key) = object_key else {
                continue;
            };
            self.object.delete_object(&object_key).await?;
            sqlx::query(&format!("DELETE FROM {TABLE_BLOB} WHERE id = $1"))
                .bind(reference.as_uuid())
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

#[cfg(test)]
mod tests {
    use super::pending_is_downloadable;

    #[test]
    fn a_pending_row_with_no_policy_and_no_expectation_is_downloadable() {
        assert!(pending_is_downloadable(false, false));
    }

    #[test]
    fn a_post_upload_policy_holds_the_pending_download_until_promotion() {
        assert!(!pending_is_downloadable(true, false));
    }

    #[test]
    fn an_expectation_holds_the_pending_download_until_promotion() {
        assert!(!pending_is_downloadable(false, true));
        assert!(!pending_is_downloadable(true, true));
    }
}
