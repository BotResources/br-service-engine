use futures_util::future::BoxFuture;
use service_engine::erase::{Erasable, Erase, Erased, PersonId};
use service_engine::full_eda;
use service_engine::impact::Dims;
use uuid::Uuid;

use super::aggregate::{Ledger, LedgerEvent};
use super::store::LedgerAggregate;
use crate::kernel::error::ReactionFault;

pub struct LedgerEraser;

impl Erasable for LedgerEraser {
    type Error = ReactionFault;

    fn erase<'a>(
        &'a self,
        cx: &'a mut Erase<'a>,
        person: PersonId,
    ) -> BoxFuture<'a, Result<Erased, ReactionFault>> {
        Box::pin(async move {
            let touched = full_eda::erase::<LedgerAggregate, _>(
                cx.connection(),
                &person,
                |person, event| match event {
                    LedgerEvent::Recorded { author, .. } if *author == person.as_uuid() => {
                        *author = Uuid::nil();
                        true
                    }
                    LedgerEvent::Recorded { .. } => false,
                },
            )
            .await?;
            let mut out = Erased::new();
            for id in &touched {
                cx.impact::<Ledger>(id, Dims::ALL)?;
            }
            out.rows(touched.len() as u64);
            Ok(out)
        })
    }
}
