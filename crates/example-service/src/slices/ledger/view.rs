use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::view::{Populate, View};
use sqlx::PgConnection;
use uuid::Uuid;

use super::aggregate::Ledger;
use super::store;
use crate::kernel::AppPrincipal;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct LedgerView {
    pub id: Uuid,
    pub total: i64,
    pub last_author: Option<Uuid>,
}

pub struct LedgerRow {
    pub id: Uuid,
    pub total: i64,
    pub last_author: Option<Uuid>,
}

#[derive(Default)]
pub struct LedgersView;

impl LedgersView {
    pub const NAME: ProjectorName = ProjectorName::from_static("ledgers");
}

impl View for LedgersView {
    type Principal = AppPrincipal;
    type Noun = Ledger;
    type Row = LedgerRow;
    type Query = ();
    type Out = LedgerView;

    const NAME: ProjectorName = Self::NAME;

    fn rows<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> service_engine::view::RowsFuture<'a, Uuid, LedgerRow> {
        Box::pin(async move {
            let mut rows = Vec::with_capacity(keys.len());
            for key in keys {
                if let Some((total, last_author)) = store::snapshot_of_conn(conn, *key).await? {
                    rows.push((
                        *key,
                        LedgerRow {
                            id: *key,
                            total,
                            last_author,
                        },
                    ));
                }
            }
            Ok(rows)
        })
    }

    fn populate<'a>(
        cx: &'a Populate<'a, AppPrincipal>,
        _query: &'a (),
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            Ok(Population::Keys(
                store::all_ledger_ids(cx.pool())
                    .await?
                    .into_iter()
                    .collect(),
            ))
        })
    }

    fn project(row: &LedgerRow, _principal: &AppPrincipal) -> LedgerView {
        LedgerView {
            id: row.id,
            total: row.total,
            last_author: row.last_author,
        }
    }
}
