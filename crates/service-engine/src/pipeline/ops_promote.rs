use std::panic::AssertUnwindSafe;

use futures_util::FutureExt as _;
use sqlx::Connection as _;
use uuid::Uuid;

use crate::blobs::policy::{UploadPolicy, Uploaded};
use crate::blobs::promotion::{expectation, mark_failed, mark_uploaded, mismatch};
use crate::blobs::{BlobHead, BlobPromotion, BlobRef, BlobRowOp, BlobState};
use crate::erase::PersonId;
use crate::error::EngineError;
use crate::pipeline::ops::Ops;
use crate::pipeline::outbound::OutboundContext;
use crate::pipeline::policy::PostSave;
use crate::pipeline::runner::{PromotionStep, classify_post_upload};
use crate::pipeline::staged::{RefusalOrigin, Staged};
use crate::schema::TABLE_BLOB;

type ReferenceRow = (
    String,
    String,
    String,
    String,
    Option<String>,
    Option<i64>,
    Option<Vec<u8>>,
);

impl Ops<'_> {
    pub(crate) async fn promote_blob(
        &mut self,
        reference: BlobRef,
    ) -> Result<BlobPromotion, EngineError> {
        let handle = self.blob_handle()?;
        let store = handle.store()?.clone();
        let id = reference.as_uuid();
        if self
            .staged
            .blob_ops
            .iter()
            .any(|op| matches!(op, BlobRowOp::Insert(row) if row.id == id))
        {
            return Ok(BlobPromotion::NotLanded);
        }
        let row: Option<ReferenceRow> = sqlx::query_as(&format!(
            "SELECT kind, service, object_key, state, failed_reason, expected_size, \
                    expected_sha256 \
             FROM {TABLE_BLOB} WHERE id = $1"
        ))
        .bind(id)
        .fetch_optional(&mut *self.conn)
        .await?;
        let Some((kind, service, object_key, state, failed_reason, size, sha)) = row else {
            return Err(not_a_reference(id));
        };
        let Some(kind) = handle.kind_named(&kind) else {
            return Err(not_a_reference(id));
        };
        if service != store.service() {
            return Err(not_a_reference(id));
        }
        if let Some(settled) = settled(id, &state, failed_reason)? {
            return Ok(settled);
        }
        let expected = expectation(size, sha)?;
        let Some(object) = store.object().head(&object_key).await? else {
            return Ok(BlobPromotion::NotLanded);
        };
        if let Some(settled) = self.lock_pending(id).await? {
            return Ok(settled);
        }
        if let Some(reason) = mismatch(&object, expected.as_ref()) {
            return self.fail_promotion(id, object_key, reason).await;
        }
        let head = BlobHead {
            state: BlobState::Uploaded,
            expected,
            size: object.size,
            etag: object.etag.clone(),
            sha256: object.sha256,
        };
        let Some(policy) = self.policies.upload.get(kind) else {
            return match mark_uploaded(&mut *self.conn, id, &head).await? {
                Some(_) => Ok(BlobPromotion::Promoted),
                None => self.reread(id).await,
            };
        };
        self.promote_under_policy(policy, reference, kind, object_key, head)
            .await
    }

    async fn promote_under_policy(
        &mut self,
        policy: UploadPolicy,
        reference: BlobRef,
        kind: &'static str,
        object_key: String,
        head: BlobHead,
    ) -> Result<BlobPromotion, EngineError> {
        let id = reference.as_uuid();
        self.refuse_a_switched_role(id).await?;
        let outbound = self.policy_outbound(reference)?;
        let mut policy_staged = Staged::default();
        let mut savepoint = self.conn.begin().await?;
        let Some((file_name, content_type, owner)) =
            mark_uploaded(&mut *savepoint, id, &head).await?
        else {
            savepoint.rollback().await?;
            return self.reread(id).await;
        };
        let uploaded = Uploaded {
            reference,
            kind,
            head: &head,
            file_name: &file_name,
            content_type: &content_type,
            owner: owner.map(PersonId),
        };
        let outcome = {
            let mut ops = Ops::new(
                &mut savepoint,
                &mut policy_staged,
                self.accumulators,
                self.offers.clone(),
                self.policies.clone(),
                self.blobs,
                self.now,
            )
            .with_outbound(outbound);
            let mut post_save = PostSave::new(&mut ops, RefusalOrigin::Upload);
            AssertUnwindSafe(policy(uploaded, &mut post_save))
                .catch_unwind()
                .await
        };
        let step = match outcome {
            Ok(outcome) => classify_post_upload(outcome),
            Err(_) => PromotionStep::Fault(EngineError::Blob(format!(
                "the post-upload policy for blob kind {kind} panicked under promote_blob"
            ))),
        };
        match step {
            PromotionStep::Commit => {
                if let Err(error) = savepoint.commit().await {
                    return Err(self.note_promotion_fault(id, error.into()));
                }
                self.staged.absorb(policy_staged);
                Ok(BlobPromotion::Promoted)
            }
            PromotionStep::Refused(code) => {
                if let Err(error) = savepoint.rollback().await {
                    return Err(self.note_promotion_fault(id, error.into()));
                }
                self.fail_promotion(id, object_key, format!("policy:{code}"))
                    .await
            }
            PromotionStep::Fault(error) => {
                let _ = savepoint.rollback().await;
                Err(self.note_promotion_fault(id, error))
            }
        }
    }

    fn note_promotion_fault(&mut self, id: Uuid, error: EngineError) -> EngineError {
        if self.staged.promotion_fault.is_none() {
            self.staged.promotion_fault = Some(format!(
                "the post-upload policy for blob {id} faulted under promote_blob: {}",
                crate::chain::describe(&error)
            ));
        }
        error
    }

    async fn lock_pending(&mut self, id: Uuid) -> Result<Option<BlobPromotion>, EngineError> {
        let row: Option<(String, Option<String>)> = sqlx::query_as(&format!(
            "SELECT state, failed_reason FROM {TABLE_BLOB} WHERE id = $1 FOR UPDATE"
        ))
        .bind(id)
        .fetch_optional(&mut *self.conn)
        .await?;
        let Some((state, failed_reason)) = row else {
            return Err(not_a_reference(id));
        };
        settled(id, &state, failed_reason)
    }

    async fn reread(&mut self, id: Uuid) -> Result<BlobPromotion, EngineError> {
        match self.lock_pending(id).await? {
            Some(settled) => Ok(settled),
            None => Err(EngineError::Blob(format!(
                "blob {id} is still pending after its promotion moved no row"
            ))),
        }
    }

    async fn fail_promotion(
        &mut self,
        id: Uuid,
        object_key: String,
        reason: String,
    ) -> Result<BlobPromotion, EngineError> {
        if mark_failed(&mut *self.conn, id, &reason).await? == 0 {
            return self.reread(id).await;
        }
        self.staged.object_deletes.push(object_key);
        Ok(BlobPromotion::Failed { reason })
    }

    async fn refuse_a_switched_role(&mut self, id: Uuid) -> Result<(), EngineError> {
        let switched: bool = sqlx::query_scalar("SELECT current_user <> session_user")
            .fetch_one(&mut *self.conn)
            .await?;
        if switched {
            return Err(EngineError::Blob(format!(
                "promote_blob for blob {id} runs the post-upload policy as the service, but the \
                 mutation's connection runs under a role the handler set; promote the blob \
                 before switching role"
            )));
        }
        Ok(())
    }

    fn policy_outbound(&self, reference: BlobRef) -> Result<OutboundContext, EngineError> {
        let producer = self.outbound()?.producer.clone();
        Ok(OutboundContext {
            actor: crate::identity::service_actor(producer.as_deref().unwrap_or("")),
            correlation_id: reference.as_uuid(),
            causation_id: None,
            producer,
        })
    }
}

fn settled(
    id: Uuid,
    state: &str,
    failed_reason: Option<String>,
) -> Result<Option<BlobPromotion>, EngineError> {
    match state {
        "pending" => Ok(None),
        "uploaded" => Ok(Some(BlobPromotion::AlreadyUploaded)),
        "failed" => Ok(Some(BlobPromotion::Failed {
            reason: failed_reason.unwrap_or_default(),
        })),
        _ => Err(EngineError::Blob(format!(
            "blob {id} is {state}; only a pending reference can be promoted"
        ))),
    }
}

fn not_a_reference(id: Uuid) -> EngineError {
    EngineError::Blob(format!(
        "blob {id} is not a reference of a blob kind this service registered"
    ))
}
