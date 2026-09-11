use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::pipeline::{Mutation, MutationInput};
use uuid::Uuid;

use super::aggregate::{Ledger, LedgerEvent, LedgerState};
use super::store::LedgerAggregate;
use crate::kernel::{AppFault, AppPrincipal};

#[derive(Debug, Deserialize)]
pub struct RecordEntry {
    pub id: Uuid,
    pub amount: i64,
}

impl MutationInput for RecordEntry {
    type Output = ();
    type Error = AppFault;
    const NAME: &'static str = "record_entry";
}

pub fn record_entry<'m>(
    cx: &'m mut Mutation<'m, AppPrincipal>,
    input: RecordEntry,
) -> BoxFuture<'m, Result<(), AppFault>> {
    Box::pin(async move {
        let org = cx.principal().org();
        let author = cx.principal().user();
        let mut ledger = match cx.load::<LedgerAggregate>(&input.id).await? {
            Some(ledger) => ledger,
            None => {
                let fresh = LedgerAggregate(LedgerState::open(input.id, org));
                cx.create(&fresh).await?;
                fresh
            }
        };
        ledger.0.record(input.amount, author)?;
        cx.save(&ledger).await?;
        cx.impact_caused::<Ledger, _>(
            &input.id,
            LedgerEvent::Recorded {
                amount: input.amount,
                author,
            },
        )?;
        Ok(())
    })
}
