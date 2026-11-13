use sqlx::PgExecutor;
use uuid::Uuid;

use crate::blobs::{BlobHead, ObjectHead, Sha256Digest, UploadExpectation};
use crate::error::EngineError;
use crate::schema::TABLE_BLOB;

/// What `Mutation::promote_blob` answers about one reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobPromotion {
    /// This call promoted the row to `uploaded`; the post-upload policy, when the kind has
    /// one, ran in the mutation's transaction and its writes commit with the mutation.
    Promoted,
    /// The row was already `uploaded` (promoted by the reaper or an earlier mutation); nothing
    /// ran and the policy did not run again.
    AlreadyUploaded,
    /// The object is not in storage yet (or the reference was staged by this same mutation);
    /// the row stays `pending` for a later promotion or the reaper.
    NotLanded,
    /// The row is `failed`: a size or SHA-256 mismatch, a policy refusal (`policy:<code>`), or
    /// a failure recorded earlier. The object is deleted after the mutation commits.
    Failed { reason: String },
}

pub(crate) type PromotedRow = (String, String, Option<Uuid>);

pub(crate) fn expectation(
    size: Option<i64>,
    sha: Option<Vec<u8>>,
) -> Result<Option<UploadExpectation>, EngineError> {
    match (size, sha) {
        (Some(size), Some(bytes)) => {
            let array: [u8; 32] = bytes.try_into().map_err(|_| {
                EngineError::Blob("a recorded expected_sha256 is not 32 bytes".into())
            })?;
            Ok(Some(UploadExpectation::new(
                size as u64,
                Sha256Digest::from_bytes(array),
            )))
        }
        _ => Ok(None),
    }
}

pub(crate) fn mismatch(
    object: &ObjectHead,
    expected: Option<&UploadExpectation>,
) -> Option<String> {
    let expected = expected?;
    if object.size != expected.size {
        return Some("size_mismatch".to_string());
    }
    if object.sha256 != Some(expected.sha256) {
        return Some("sha256_mismatch".to_string());
    }
    None
}

/// Moves a `pending` row to `uploaded`. Holds only for a `pending` row, which is what makes a
/// promotion happen once between the reaper and a mutation.
pub(crate) async fn mark_uploaded<'e>(
    executor: impl PgExecutor<'e>,
    id: Uuid,
    head: &BlobHead,
) -> Result<Option<PromotedRow>, sqlx::Error> {
    sqlx::query_as(&format!(
        "UPDATE {TABLE_BLOB} \
         SET state = 'uploaded', uploaded_at = now(), size = $2, etag = $3, sha256 = $4 \
         WHERE id = $1 AND state = 'pending' \
         RETURNING file_name, content_type, owner_id"
    ))
    .bind(id)
    .bind(head.size as i64)
    .bind(&head.etag)
    .bind(head.sha256.map(|digest| digest.as_bytes().to_vec()))
    .fetch_optional(executor)
    .await
}

pub(crate) async fn mark_failed<'e>(
    executor: impl PgExecutor<'e>,
    id: Uuid,
    reason: &str,
) -> Result<u64, sqlx::Error> {
    let done = sqlx::query(&format!(
        "UPDATE {TABLE_BLOB} \
         SET state = 'failed', failed_reason = $2, failed_at = now() \
         WHERE id = $1 AND state = 'pending'"
    ))
    .bind(id)
    .bind(reason)
    .execute(executor)
    .await?;
    Ok(done.rows_affected())
}
