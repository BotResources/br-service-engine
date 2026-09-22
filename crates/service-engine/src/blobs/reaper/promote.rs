use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::{BATCH, BlobReaper, ReaperRound};
use crate::blobs::head::{BlobHead, BlobState};
use crate::blobs::policy::Uploaded;
use crate::blobs::store::BlobStore;
use crate::blobs::{BlobRef, ObjectHead, Sha256Digest, UploadExpectation};
use crate::erase::PersonId;
use crate::error::EngineError;
use crate::pipeline::PromotionOutcome;
use crate::schema::TABLE_BLOB;

impl BlobReaper {
    pub(super) async fn promote_pending(
        &self,
        pg: &PgPool,
        store: &BlobStore,
        kind: &'static str,
        cutoff: chrono::DateTime<chrono::Utc>,
        round: &mut ReaperRound,
    ) -> Result<(), EngineError> {
        let rows = sqlx::query(&format!(
            "SELECT id, object_key, created_at, expected_size, expected_sha256 \
             FROM {TABLE_BLOB} WHERE kind = $1 AND state = 'pending' LIMIT $2"
        ))
        .bind(kind)
        .bind(BATCH)
        .fetch_all(pg)
        .await?;
        for row in &rows {
            let id: Uuid = row.get("id");
            let object_key: String = row.get("object_key");
            let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");
            let expected = expectation(row)?;
            match store.object().head(&object_key).await? {
                None if created_at < cutoff => {
                    let done = sqlx::query(&format!(
                        "DELETE FROM {TABLE_BLOB} WHERE id = $1 AND state = 'pending'"
                    ))
                    .bind(id)
                    .execute(pg)
                    .await?;
                    round.reaped_incomplete += done.rows_affected();
                }
                None => {}
                Some(object) => {
                    if let Some(reason) = mismatch(&object, expected.as_ref()) {
                        self.fail_and_delete(pg, store, id, &object_key, reason, round)
                            .await?;
                        continue;
                    }
                    let head = BlobHead {
                        state: BlobState::Uploaded,
                        expected,
                        size: object.size,
                        etag: object.etag.clone(),
                        sha256: object.sha256,
                    };
                    self.promote_one(pg, store, kind, id, &object_key, &head, round)
                        .await?;
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn promote_one(
        &self,
        pg: &PgPool,
        store: &BlobStore,
        kind: &'static str,
        id: Uuid,
        object_key: &str,
        head: &BlobHead,
        round: &mut ReaperRound,
    ) -> Result<(), EngineError> {
        let mut tx = pg.begin().await?;
        let promoted: Option<(String, String, Option<Uuid>)> = sqlx::query_as(&format!(
            "UPDATE {TABLE_BLOB} \
             SET state = 'uploaded', uploaded_at = now(), size = $2, etag = $3, sha256 = $4 \
             WHERE id = $1 AND state = 'pending' \
             RETURNING file_name, content_type, owner_id"
        ))
        .bind(id)
        .bind(head.size as i64)
        .bind(&head.etag)
        .bind(head.sha256.map(|digest| digest.as_bytes().to_vec()))
        .fetch_optional(&mut *tx)
        .await?;
        let Some((file_name, content_type, owner)) = promoted else {
            let _ = tx.rollback().await;
            return Ok(());
        };
        let Some(runner) = self.runner.as_ref() else {
            tx.commit().await?;
            round.promoted += 1;
            return Ok(());
        };
        let uploaded = Uploaded {
            reference: BlobRef(id),
            kind,
            head,
            file_name: &file_name,
            content_type: &content_type,
            owner: owner.map(PersonId),
        };
        match runner.run_after_promotion(tx, uploaded).await? {
            PromotionOutcome::Committed => round.promoted += 1,
            PromotionOutcome::Refused(code) => {
                self.fail_and_delete(pg, store, id, object_key, format!("policy:{code}"), round)
                    .await?;
            }
        }
        Ok(())
    }

    async fn fail_and_delete(
        &self,
        pg: &PgPool,
        store: &BlobStore,
        id: Uuid,
        object_key: &str,
        reason: String,
        round: &mut ReaperRound,
    ) -> Result<(), EngineError> {
        let done = sqlx::query(&format!(
            "UPDATE {TABLE_BLOB} \
             SET state = 'failed', failed_reason = $2, failed_at = now() \
             WHERE id = $1 AND state = 'pending'"
        ))
        .bind(id)
        .bind(&reason)
        .execute(pg)
        .await?;
        if done.rows_affected() == 0 {
            return Ok(());
        }
        store.object().delete_object(object_key).await?;
        round.failed += done.rows_affected();
        Ok(())
    }

    pub(super) async fn reap_detached(
        &self,
        pg: &PgPool,
        store: &BlobStore,
        kind: &'static str,
        cutoff: chrono::DateTime<chrono::Utc>,
        round: &mut ReaperRound,
    ) -> Result<(), EngineError> {
        let rows = sqlx::query(&format!(
            "SELECT id, object_key FROM {TABLE_BLOB} \
             WHERE kind = $1 AND ((state = 'orphaned' AND detached_at < $2) \
                                  OR (state = 'failed' AND failed_at < $2)) LIMIT $3"
        ))
        .bind(kind)
        .bind(cutoff)
        .bind(BATCH)
        .fetch_all(pg)
        .await?;
        for row in &rows {
            let id: Uuid = row.get("id");
            let object_key: String = row.get("object_key");
            store.object().delete_object(&object_key).await?;
            let done = sqlx::query(&format!(
                "DELETE FROM {TABLE_BLOB} WHERE id = $1 AND state IN ('orphaned', 'failed')"
            ))
            .bind(id)
            .execute(pg)
            .await?;
            round.reaped_orphan += done.rows_affected();
        }
        Ok(())
    }
}

fn expectation(row: &sqlx::postgres::PgRow) -> Result<Option<UploadExpectation>, EngineError> {
    let size: Option<i64> = row.get("expected_size");
    let sha: Option<Vec<u8>> = row.get("expected_sha256");
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

fn mismatch(object: &ObjectHead, expected: Option<&UploadExpectation>) -> Option<String> {
    let expected = expected?;
    if object.size != expected.size {
        return Some("size_mismatch".to_string());
    }
    if object.sha256 != Some(expected.sha256) {
        return Some("sha256_mismatch".to_string());
    }
    None
}
