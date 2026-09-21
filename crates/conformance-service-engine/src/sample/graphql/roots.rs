use async_graphql::{Context, Error, Json, MergedObject, Object, Result, Subscription};
use futures_util::{Stream, StreamExt};
use service_engine::session::{WindowParams, WindowSpec};
use service_engine::{MutationAck, Query};
use uuid::Uuid;

use crate::sample::assignment::{AssignmentProjector, AssignmentView};
use crate::sample::pipeline::{CloseWidget, MintSecret};
use crate::sample::principal::SamplePrincipal;
use crate::sample::widget::{WidgetProjector, WidgetView};

service_engine::subscription_union! {
    view = ProjectedView;
    delta = EngineDelta { reset = ResetPayload, upsert = UpsertPayload, remove = RemovePayload };
    Widget => WidgetProjector => WidgetView,
    Assignment => AssignmentProjector => AssignmentView,
}

#[derive(Default)]
pub struct WidgetQueries;

#[Object]
impl WidgetQueries {
    async fn sample_widget(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
    ) -> Result<Option<Json<serde_json::Value>>> {
        let view = Query::<SamplePrincipal>::new(ctx)?
            .fetch::<WidgetProjector>(&id)
            .await?;
        match view {
            Some(view) => Ok(Some(Json(
                serde_json::to_value(view).map_err(|e| Error::new(e.to_string()))?,
            ))),
            None => Ok(None),
        }
    }
}

#[derive(Default)]
pub struct AssignmentQueries;

#[Object]
impl AssignmentQueries {
    async fn sample_assignment(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
    ) -> Result<Option<AssignmentView>> {
        Query::<SamplePrincipal>::new(ctx)?
            .fetch::<AssignmentProjector>(&id)
            .await
    }
}

#[derive(MergedObject, Default)]
pub struct QueryRoot(WidgetQueries, AssignmentQueries);

#[derive(Default)]
pub struct MutationRoot;

#[Object]
impl MutationRoot {
    async fn sample_close_widget(&self, ctx: &Context<'_>, id: Uuid) -> Result<MutationAck> {
        service_engine::ack::<SamplePrincipal, CloseWidget>(ctx, CloseWidget { id }).await
    }

    async fn sample_mint_secret(
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
    async fn sample_widgets(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = Result<EngineDelta>>> {
        let stream = service_engine::attach::<SamplePrincipal>(
            ctx,
            vec![WindowSpec::new(
                WidgetProjector::NAME,
                WindowParams::none(),
                false,
            )],
        )
        .await?;
        Ok(stream.map(|delta| EngineDelta::from_delta(&delta)))
    }

    async fn sample_assignments(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = Result<EngineDelta>>> {
        let stream = service_engine::attach::<SamplePrincipal>(
            ctx,
            vec![WindowSpec::new(
                AssignmentProjector::NAME,
                WindowParams::none(),
                false,
            )],
        )
        .await?;
        Ok(stream.map(|delta| EngineDelta::from_delta(&delta)))
    }
}
