use async_graphql::{Context, Json, MergedObject, Object, Result, Subscription};
use futures_util::Stream;
use service_engine::session::{WindowParams, WindowSpec};
use service_engine::{EngineDelta, MutationAck};
use uuid::Uuid;

use crate::sample::assignment::AssignmentProjector;
use crate::sample::pipeline::{CloseWidget, MintSecret};
use crate::sample::principal::SamplePrincipal;
use crate::sample::widget::WidgetProjector;

#[derive(Default)]
pub struct WidgetQueries;

#[Object]
impl WidgetQueries {
    async fn widget(&self, ctx: &Context<'_>, id: Uuid) -> Result<Option<Json<serde_json::Value>>> {
        service_engine::fetch_json::<SamplePrincipal, WidgetProjector>(
            ctx,
            &WidgetProjector,
            &id,
            false,
        )
        .await
    }
}

#[derive(Default)]
pub struct AssignmentQueries;

#[Object]
impl AssignmentQueries {
    async fn assignment(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
    ) -> Result<Option<Json<serde_json::Value>>> {
        service_engine::fetch_json::<SamplePrincipal, AssignmentProjector>(
            ctx,
            &AssignmentProjector,
            &id,
            false,
        )
        .await
    }
}

#[derive(MergedObject, Default)]
pub struct QueryRoot(WidgetQueries, AssignmentQueries);

#[derive(Default)]
pub struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn close_widget(&self, ctx: &Context<'_>, id: Uuid) -> Result<MutationAck> {
        service_engine::ack::<SamplePrincipal, CloseWidget>(ctx, CloseWidget { id }).await
    }

    async fn mint_secret(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
        label: String,
        tenant: Uuid,
    ) -> Result<String> {
        let secret = service_engine::execute::<SamplePrincipal, MintSecret>(
            ctx,
            MintSecret { id, label, tenant },
        )
        .await?;
        Ok(secret.into_inner())
    }
}

#[derive(Default)]
pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    async fn widgets(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = Result<EngineDelta>>> {
        service_engine::subscribe::<SamplePrincipal>(
            ctx,
            vec![WindowSpec::new(
                WidgetProjector::NAME,
                WindowParams::none(),
                false,
            )],
        )
        .await
    }
}
