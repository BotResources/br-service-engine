use std::collections::HashMap;

use service_engine::error::EngineError;
use service_engine::mirror::{Column, KnownRow, PrincipalColumn, Projection, RowScope, col};
use uuid::Uuid;

use super::USER_NAMESPACE;

pub struct KnownUserRow {
    pub id: Uuid,
    pub email: String,
}

impl KnownRow for KnownUserRow {
    const TABLE: &'static str = "known_users";
    const NAMESPACE: &'static str = USER_NAMESPACE;
    const KEY: &'static [&'static str] = &["user_id"];
    const PRINCIPAL: Option<PrincipalColumn> = None;

    fn key(&self) -> Vec<Column> {
        vec![col("user_id", self.id)]
    }

    fn values(&self) -> Vec<Column> {
        vec![col("email", self.email.clone())]
    }
}

pub async fn replace_known_users(
    cx: &mut Projection<'_>,
    batch: Vec<Uuid>,
    emails: &HashMap<Uuid, String>,
) -> Result<(), EngineError> {
    let rows: Vec<KnownUserRow> = batch
        .iter()
        .filter_map(|id| {
            emails.get(id).map(|email| KnownUserRow {
                id: *id,
                email: email.clone(),
            })
        })
        .collect();
    cx.replace_rows(RowScope::any_of("user_id", batch), rows)
        .await
        .map(|_| ())
}
