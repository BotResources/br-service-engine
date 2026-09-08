use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::nats::{KvBucket, KvKey, Nats};
use service_engine::offer::Offer;
use service_engine::pipeline::{Mutation, MutationInput};
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::sample::pipeline::SampleFault;
use crate::sample::principal::SamplePrincipal;
use crate::sample::widget::{Widget, WidgetRow};

pub const WIDGET_OFFER: &str = "widget_v1";
pub const WIDGET_OFFER_PREFIX: &str = "sample/widgets/v1/";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedWidget {
    pub id: Uuid,
    pub label: String,
}

pub struct WidgetOffer;

impl Offer for WidgetOffer {
    type Row = WidgetRow;
    type Published = PublishedWidget;

    const NAME: &'static str = WIDGET_OFFER;
    const PREFIX: &'static str = WIDGET_OFFER_PREFIX;

    fn key(row: &WidgetRow) -> Result<KvKey, EngineError> {
        widget_key(row.id)
    }

    fn publish(row: &WidgetRow) -> Option<PublishedWidget> {
        (!row.closed).then(|| PublishedWidget {
            id: row.id,
            label: row.label.clone(),
        })
    }

    fn all(conn: &mut PgConnection) -> BoxFuture<'_, Result<Vec<WidgetRow>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query("SELECT id, tenant_id, label, closed FROM sample_widget")
                .fetch_all(conn)
                .await?;
            Ok(rows
                .into_iter()
                .map(|row| WidgetRow {
                    id: row.get("id"),
                    tenant_id: row.get("tenant_id"),
                    label: row.get("label"),
                    closed: row.get("closed"),
                })
                .collect())
        })
    }
}

pub fn widget_key(id: Uuid) -> Result<KvKey, EngineError> {
    KvKey::new(format!("{WIDGET_OFFER_PREFIX}{id}"))
        .map_err(|error| EngineError::Config(format!("sample offer key: {error}")))
}

#[derive(Debug, Deserialize)]
pub struct MintThenReject {
    pub id: Uuid,
    pub tenant: Uuid,
    pub label: String,
}

impl MutationInput for MintThenReject {
    type Output = ();
    type Error = SampleFault;
    const NAME: &'static str = "mint_then_reject";
}

pub fn mint_then_reject<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: MintThenReject,
) -> BoxFuture<'m, Result<(), SampleFault>> {
    Box::pin(async move {
        let widget = WidgetRow {
            id: input.id,
            tenant_id: input.tenant,
            label: input.label,
            closed: false,
        };
        cx.create(&widget).await?;
        cx.impact_caused::<Widget, _>(&widget.id, "minted")?;
        Err(SampleFault::NotFound)
    })
}

pub async fn offer_bucket(nats: &Nats) -> KvBucket<PublishedWidget> {
    nats.published_language::<PublishedWidget>()
        .await
        .expect("bind the published-language bucket")
}

pub async fn published_widget(nats: &Nats, id: Uuid) -> Option<PublishedWidget> {
    let key = widget_key(id).expect("a valid offer key");
    offer_bucket(nats)
        .await
        .get(&key)
        .await
        .expect("read the offered widget")
}

pub async fn seed_bucket(nats: &Nats, id: Uuid, label: &str) {
    let key = widget_key(id).expect("a valid offer key");
    offer_bucket(nats)
        .await
        .put(
            &key,
            &PublishedWidget {
                id,
                label: label.to_string(),
            },
        )
        .await
        .expect("seed the offer bucket the way a stale run would have");
}

pub async fn offer_dirty_keys(pool: &PgPool, id: Uuid) -> i64 {
    let key = widget_key(id).expect("a valid offer key");
    sqlx::query_scalar("SELECT count(*) FROM service_engine.offer_dirty WHERE kv_key = $1")
        .bind(key.as_str())
        .fetch_one(pool)
        .await
        .expect("count the staged dirty offer keys")
}
