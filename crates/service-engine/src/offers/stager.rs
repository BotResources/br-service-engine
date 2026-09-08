use std::any::{Any, TypeId};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::error::EngineError;
use crate::nats::KvKey;
use crate::offer::Offer;
use crate::persistence::Aggregate;
use crate::wire::KeyBytes;

pub(crate) struct OfferDirty {
    pub offer: &'static str,
    pub kv_key: KvKey,
    pub agg_key: KeyBytes,
}

trait ErasedStager: Send + Sync {
    fn stage(&self, agg: &dyn Any, out: &mut Vec<OfferDirty>) -> Result<(), EngineError>;
}

struct StagerFor<O>(PhantomData<fn() -> O>);

impl<O: Offer> ErasedStager for StagerFor<O> {
    fn stage(&self, agg: &dyn Any, out: &mut Vec<OfferDirty>) -> Result<(), EngineError> {
        let row = agg.downcast_ref::<O::Row>().ok_or_else(|| {
            EngineError::Config(format!(
                "the offer {} was handed an aggregate of the wrong type",
                O::NAME
            ))
        })?;
        out.push(OfferDirty {
            offer: O::NAME,
            kv_key: O::key(row)?,
            agg_key: KeyBytes::encode(&row.key())?,
        });
        Ok(())
    }
}

#[derive(Default, Clone)]
pub(crate) struct OfferStagers {
    by_row: BTreeMap<TypeId, Vec<Arc<dyn ErasedStager>>>,
}

impl OfferStagers {
    pub(crate) fn register<O: Offer>(&mut self) {
        self.by_row
            .entry(TypeId::of::<O::Row>())
            .or_default()
            .push(Arc::new(StagerFor::<O>(PhantomData)));
    }

    pub(crate) fn stage_for<A: Aggregate>(
        &self,
        aggregate: &A,
        out: &mut Vec<OfferDirty>,
    ) -> Result<(), EngineError> {
        if let Some(stagers) = self.by_row.get(&TypeId::of::<A>()) {
            let any: &dyn Any = aggregate;
            for stager in stagers {
                stager.stage(any, out)?;
            }
        }
        Ok(())
    }
}
