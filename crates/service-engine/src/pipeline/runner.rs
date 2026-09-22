use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures_util::future::FutureExt as _;
use sqlx::{Postgres, Transaction};

use crate::accumulator::AccumulatorRuntime;
use crate::blobs::policy::Uploaded;
use crate::blobs::{BlobHandle, BlobRef, PostUpload};
use crate::error::EngineError;
use crate::offers::OfferStagers;
use crate::pipeline::ops::Ops;
use crate::pipeline::outbound::OutboundContext;
use crate::pipeline::staged::{RefusalOrigin, Staged};
use crate::pipeline::tx::flush_and_commit;
use crate::pipeline::{Policies, PostSave, Refused};
use crate::time;
use crate::transport::ImpactTransport;

pub(crate) enum PromotionOutcome {
    Committed,
    Refused(&'static str),
}

pub(crate) enum PromotionStep {
    Commit,
    Refused(&'static str),
    Fault(EngineError),
}

pub(crate) fn classify_post_upload(outcome: Result<(), PostUpload>) -> PromotionStep {
    match outcome {
        Ok(()) => PromotionStep::Commit,
        Err(PostUpload::Refused(Refused(reason))) => PromotionStep::Refused(reason.code()),
        Err(PostUpload::Fault(error)) => PromotionStep::Fault(error),
    }
}

impl std::fmt::Debug for PromotionStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Commit => f.write_str("Commit"),
            Self::Refused(code) => write!(f, "Refused({code})"),
            Self::Fault(error) => write!(f, "Fault({error})"),
        }
    }
}

pub(crate) struct PolicyRunner {
    transport: Arc<dyn ImpactTransport>,
    accumulators: Arc<AccumulatorRuntime>,
    offers: Arc<OfferStagers>,
    policies: Arc<Policies>,
    blobs: BlobHandle,
    service: Option<String>,
    impacts_per_commit: usize,
}

impl PolicyRunner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        transport: Arc<dyn ImpactTransport>,
        accumulators: Arc<AccumulatorRuntime>,
        offers: Arc<OfferStagers>,
        policies: Arc<Policies>,
        blobs: BlobHandle,
        service: Option<String>,
        impacts_per_commit: usize,
    ) -> Self {
        Self {
            transport,
            accumulators,
            offers,
            policies,
            blobs,
            service,
            impacts_per_commit,
        }
    }

    fn outbound(&self, reference: BlobRef) -> OutboundContext {
        let actor = crate::identity::service_actor(self.service.as_deref().unwrap_or(""));
        OutboundContext {
            actor,
            correlation_id: reference.as_uuid(),
            causation_id: None,
            producer: self.service.clone(),
        }
    }

    pub(crate) async fn run_after_promotion(
        &self,
        mut tx: Transaction<'static, Postgres>,
        uploaded: Uploaded<'_>,
    ) -> Result<PromotionOutcome, EngineError> {
        let Some(policy) = self.policies.upload.get(uploaded.kind) else {
            tx.commit().await.map_err(EngineError::from)?;
            return Ok(PromotionOutcome::Committed);
        };
        let reference = uploaded.reference;
        let kind = uploaded.kind;
        let outbound = self.outbound(reference);
        let mut staged = Staged::default();
        let outcome = {
            let mut ops = Ops::new(
                &mut tx,
                &mut staged,
                self.accumulators.as_ref(),
                self.offers.clone(),
                self.policies.clone(),
                Some(&self.blobs),
                time::now(),
            )
            .with_outbound(outbound);
            let mut post_save = PostSave::new(&mut ops, RefusalOrigin::Upload);
            AssertUnwindSafe(policy(uploaded, &mut post_save))
                .catch_unwind()
                .await
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(_) => {
                let _ = tx.rollback().await;
                return Err(EngineError::Blob(format!(
                    "a post-upload policy for blob kind {kind} panicked; the row stays pending \
                     for the next sweep rather than taking the beat down"
                )));
            }
        };
        match classify_post_upload(outcome) {
            PromotionStep::Commit => {}
            PromotionStep::Refused(code) => {
                let _ = tx.rollback().await;
                return Ok(PromotionOutcome::Refused(code));
            }
            PromotionStep::Fault(error) => {
                let _ = tx.rollback().await;
                return Err(error);
            }
        }
        if !staged.is_within(self.impacts_per_commit) {
            let _ = tx.rollback().await;
            return Err(EngineError::Config(format!(
                "a post-upload policy dirtied {} keys, over impacts_per_commit={}",
                staged.impacts.len(),
                self.impacts_per_commit
            )));
        }
        flush_and_commit(tx, &staged, self.transport.as_ref()).await?;
        self.accumulators
            .purge_committed_seals(&staged.sealed_keys)
            .await;
        Ok(PromotionOutcome::Committed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate::Reason;

    #[test]
    fn an_ok_policy_outcome_commits() {
        assert!(matches!(
            classify_post_upload(Ok(())),
            PromotionStep::Commit
        ));
    }

    #[test]
    fn a_refusal_is_terminal_and_carries_its_code() {
        let step = classify_post_upload(Err(PostUpload::Refused(Refused(Reason::new("NOPE")))));
        assert!(matches!(step, PromotionStep::Refused("NOPE")));
    }

    #[test]
    fn a_deterministic_fault_carries_the_engine_error_for_a_retry_not_a_refusal() {
        let step = classify_post_upload(Err(PostUpload::Fault(EngineError::Blob(
            "transient".into(),
        ))));
        match step {
            PromotionStep::Fault(EngineError::Blob(detail)) => assert_eq!(detail, "transient"),
            other => panic!("a fault must classify as Fault, not commit or refuse: {other:?}"),
        }
    }
}
