use std::sync::Arc;

use sqlx::{Postgres, Transaction};

use crate::accumulator::AccumulatorRuntime;
use crate::blobs::policy::Uploaded;
use crate::blobs::{BlobHandle, BlobRef};
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
            policy(uploaded, &mut post_save).await
        };
        if let Err(Refused(reason)) = outcome {
            let _ = tx.rollback().await;
            return Ok(PromotionOutcome::Refused(reason.code()));
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
