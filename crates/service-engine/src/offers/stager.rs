use std::any::{Any, TypeId};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::error::EngineError;
use crate::nats::KvKey;
use crate::offer::{Offer, OfferTrigger};
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

struct RowStager<O>(PhantomData<fn() -> O>);

impl<O: Offer> ErasedStager for RowStager<O> {
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

struct TriggerStager<O, T>(PhantomData<fn() -> (O, T)>);

impl<O, T> ErasedStager for TriggerStager<O, T>
where
    O: Offer,
    T: OfferTrigger<O>,
{
    fn stage(&self, agg: &dyn Any, out: &mut Vec<OfferDirty>) -> Result<(), EngineError> {
        let trigger = agg.downcast_ref::<T>().ok_or_else(|| {
            EngineError::Config(format!(
                "the offer {} trigger was handed an aggregate of the wrong type",
                O::NAME
            ))
        })?;
        let row_key = trigger.row_key()?;
        out.push(OfferDirty {
            offer: O::NAME,
            kv_key: trigger.key_from()?,
            agg_key: KeyBytes::encode(&row_key)?,
        });
        Ok(())
    }
}

#[derive(Default, Clone)]
pub(crate) struct OfferStagers {
    by_aggregate: BTreeMap<TypeId, Vec<Arc<dyn ErasedStager>>>,
}

impl OfferStagers {
    pub(crate) fn register<O: Offer>(&mut self) {
        self.by_aggregate
            .entry(TypeId::of::<O::Row>())
            .or_default()
            .push(Arc::new(RowStager::<O>(PhantomData)));
    }

    pub(crate) fn register_trigger<O, T>(&mut self) -> Result<(), EngineError>
    where
        O: Offer,
        T: OfferTrigger<O>,
    {
        if TypeId::of::<T>() == TypeId::of::<O::Row>() {
            return Err(EngineError::Config(format!(
                "the offer {} trigger is its own row aggregate; register the row with \
                 register_offer, not a trigger with register_offer_trigger",
                O::NAME
            )));
        }
        self.by_aggregate
            .entry(TypeId::of::<T>())
            .or_default()
            .push(Arc::new(TriggerStager::<O, T>(PhantomData)));
        Ok(())
    }

    pub(crate) fn stage_for<A: Aggregate>(
        &self,
        aggregate: &A,
        out: &mut Vec<OfferDirty>,
    ) -> Result<(), EngineError> {
        if let Some(stagers) = self.by_aggregate.get(&TypeId::of::<A>()) {
            let any: &dyn Any = aggregate;
            for stager in stagers {
                stager.stage(any, out)?;
            }
        }
        Ok(())
    }
}
