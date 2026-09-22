use std::sync::Arc;

use crate::accumulator::AccumulatorRuntime;
use crate::blobs::{BlobHandle, BlobRegistry, BoundBlobs};
use crate::config::EngineConfig;
use crate::error::EngineError;
use crate::housekeeping::beat::Beat;
use crate::offers::OfferStagers;
use crate::pipeline::{Policies, PolicyRunner};
use crate::transport::{ImpactTransport, PgListenNotify};

#[allow(clippy::too_many_arguments)]
pub(super) async fn bind_blobs(
    registry: &BlobRegistry,
    config: &EngineConfig,
    beat: Beat,
    transport: &Arc<PgListenNotify>,
    accumulators: &Arc<AccumulatorRuntime>,
    offers: &Arc<OfferStagers>,
    policies: &Arc<Policies>,
    round_log: Option<crate::blobs::ReaperRoundLog>,
) -> Result<(Option<BlobHandle>, Beat), (EngineError, &'static str)> {
    let mut beat = beat;
    let policy_kinds = policies.upload.kinds();
    let BoundBlobs { handle, reaper } = crate::blobs::bind(registry, config, policy_kinds).await?;
    if let Some(mut reaper) = reaper {
        if let Some(blob_handle) = handle.clone() {
            reaper = reaper.with_runner(PolicyRunner::new(
                transport.clone() as Arc<dyn ImpactTransport>,
                accumulators.clone(),
                offers.clone(),
                policies.clone(),
                blob_handle,
                config.service.clone(),
                config.impacts_per_commit,
            ));
        }
        if let Some(log) = round_log {
            reaper = reaper.with_round_log(log);
        }
        beat = beat.with_blob_reaper(reaper);
    }
    Ok((handle, beat))
}
