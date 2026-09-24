use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::nats::KvKey;
use service_engine::offer::OfferTrigger;
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle, RowBatch};
use service_engine::pipeline::{Mutation, MutationInput};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::sample::offer::{WidgetOffer, widget_key};
use crate::sample::pipeline::SampleFault;
use crate::sample::principal::SamplePrincipal;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WidgetTag {
    pub widget_id: Uuid,
    pub tag: String,
}

pub struct WidgetTagStore;

impl Persistence for WidgetTagStore {
    type Aggregate = WidgetTag;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> RowBatch<'a, Uuid, WidgetTag> {
        Box::pin(async move {
            let rows = sqlx::query(
                "SELECT widget_id, tag FROM sample_widget_tag WHERE widget_id = ANY($1)",
            )
            .bind(keys)
            .fetch_all(conn)
            .await?;
            Ok(rows
                .into_iter()
                .map(|row| {
                    let tag = WidgetTag {
                        widget_id: row.get("widget_id"),
                        tag: row.get("tag"),
                    };
                    (tag.widget_id, tag)
                })
                .collect())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a WidgetTag,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO sample_widget_tag (widget_id, tag) VALUES ($1, $2) \
                 ON CONFLICT (widget_id) DO UPDATE SET tag = EXCLUDED.tag",
            )
            .bind(aggregate.widget_id)
            .bind(&aggregate.tag)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a WidgetTag,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("INSERT INTO sample_widget_tag (widget_id, tag) VALUES ($1, $2)")
                .bind(aggregate.widget_id)
                .bind(&aggregate.tag)
                .execute(conn)
                .await?;
            Ok(())
        })
    }
}

impl Aggregate for WidgetTag {
    type Store = WidgetTagStore;

    fn key(&self) -> Uuid {
        self.widget_id
    }
}

impl OfferTrigger<WidgetOffer> for WidgetTag {
    fn key_from(&self) -> Result<KvKey, EngineError> {
        widget_key(self.widget_id)
    }

    fn row_key(&self) -> Result<Uuid, EngineError> {
        Ok(self.widget_id)
    }
}

#[derive(Debug, Deserialize)]
pub struct SetWidgetTag {
    pub widget_id: Uuid,
    pub tag: String,
}

impl MutationInput for SetWidgetTag {
    type Output = ();
    type Error = SampleFault;
    const NAME: &'static str = "set_widget_tag";
}

pub fn set_widget_tag<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: SetWidgetTag,
) -> BoxFuture<'m, Result<(), SampleFault>> {
    Box::pin(async move {
        let tag = WidgetTag {
            widget_id: input.widget_id,
            tag: input.tag,
        };
        cx.create(&tag).await?;
        Ok(())
    })
}

#[derive(Debug, Deserialize)]
pub struct DeleteWidgetTag {
    pub widget_id: Uuid,
}

impl MutationInput for DeleteWidgetTag {
    type Output = ();
    type Error = SampleFault;
    const NAME: &'static str = "delete_widget_tag";
}

pub fn delete_widget_tag<'m>(
    cx: &'m mut Mutation<'m, SamplePrincipal>,
    input: DeleteWidgetTag,
) -> BoxFuture<'m, Result<(), SampleFault>> {
    Box::pin(async move {
        let tag = cx
            .load::<WidgetTag>(&input.widget_id)
            .await?
            .ok_or(SampleFault::NotFound)?;
        cx.delete(&tag).await?;
        Ok(())
    })
}
