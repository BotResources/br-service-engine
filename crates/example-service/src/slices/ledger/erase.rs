use futures_util::future::BoxFuture;
use service_engine::erase::{Erasable, Erase, Erased, PersonId};
use service_engine::impact::Dims;
use sqlx::Row;
use uuid::Uuid;

use super::aggregate::Ledger;
use super::store;
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
            let author = person.as_uuid();
            let touched: Vec<Uuid> =
                sqlx::query("SELECT DISTINCT id FROM ledger_event WHERE author = $1")
                    .bind(author)
                    .fetch_all(cx.connection())
                    .await?
                    .iter()
                    .map(|row| row.get::<Uuid, _>("id"))
                    .collect();
            let rewritten = store::erase_author(cx.connection(), author).await?;
            let mut out = Erased::new();
            for id in &touched {
                cx.impact::<Ledger>(id, Dims::ALL)?;
            }
            out.rows(rewritten);
            Ok(out)
        })
    }
}
