use futures_util::future::BoxFuture;
use serde::Deserialize;
use service_engine::pipeline::{Mutation, MutationInput};
use uuid::Uuid;

use super::aggregate::{Ledger, LedgerState};
use super::store::{LedgerAggregate, snapshot_of_conn};
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
        let existing = snapshot_of_conn(cx.connection(), input.id).await?;
        if existing.is_none() {
            let ledger = LedgerAggregate(LedgerState::open(input.id, org));
            cx.create(&ledger).await?;
        }
        let mut ledger = cx
            .load::<LedgerAggregate>(&input.id)
            .await?
            .ok_or(AppFault::NotFound)?;
        ledger.0.record(input.amount, author)?;
        cx.save(&ledger).await?;
        cx.impact_caused::<Ledger, _>(&input.id, "recorded")?;
        Ok(())
    })
}
