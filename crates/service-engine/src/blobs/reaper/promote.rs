use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::{BATCH, BlobReaper, ReaperRound};
use crate::blobs::head::{BlobHead, BlobState};
use crate::blobs::policy::Uploaded;
use crate::blobs::promotion::{expectation, mark_failed, mark_uploaded, mismatch};
use crate::blobs::store::BlobStore;
use crate::blobs::{BlobRef, UploadExpectation};
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
            let expected = expectation_of(row)?;
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
        let promoted = mark_uploaded(&mut *tx, id, head).await?;
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
        match runner.run_after_promotion(tx, uploaded).await {
            Ok(PromotionOutcome::Committed) => round.promoted += 1,
            Ok(PromotionOutcome::Refused(code)) => {
                self.fail_and_delete(pg, store, id, object_key, format!("policy:{code}"), round)
                    .await?;
            }
            Err(error) => {
                round.failures += 1;
                tracing::warn!(
                    blob = %id,
                    reason = %crate::chain::describe(&error),
                    "a post-upload policy faulted; the row stays pending for the next sweep",
                );
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
        let failed = mark_failed(pg, id, &reason).await?;
        if failed == 0 {
            return Ok(());
        }
        if let Err(error) = store.object().delete_object(object_key).await {
            tracing::warn!(
                reason = %crate::chain::describe(&error),
                "a failed blob object could not be deleted; reap_detached finishes it later"
            );
        }
        round.failed += failed;
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

fn expectation_of(row: &sqlx::postgres::PgRow) -> Result<Option<UploadExpectation>, EngineError> {
    expectation(row.get("expected_size"), row.get("expected_sha256"))
}
