use async_graphql::{Context, Json, Object, Result};
use service_engine::graphql::SliceFragment;
use service_engine::nats::Nats;
use service_engine::session::WindowParams;
use service_engine::{Query, ViewProjector};
use uuid::Uuid;

use crate::infra::TestDb;
use crate::sample::assignment::AssignmentView;
use crate::sample::counted::CountedAssignments;
use crate::sample::principal::SamplePrincipal;

use super::boot::{GraphqlService, base_config};
use super::query_service::boot_query_service;

pub struct CountedQueryRoot;

#[Object]
impl CountedQueryRoot {
    async fn sample_counted_window(&self, ctx: &Context<'_>) -> Result<Vec<AssignmentView>> {
        Query::<SamplePrincipal>::new(ctx)?
            .fetch_view_window::<CountedAssignments>(&())
            .await
    }

    async fn sample_counted_window_json(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Vec<Json<serde_json::Value>>> {
        Query::<SamplePrincipal>::new(ctx)?
            .fetch_window_json::<ViewProjector<CountedAssignments>>(WindowParams::none())
            .await
    }

    async fn sample_counted_item(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
    ) -> Result<Option<AssignmentView>> {
        Query::<SamplePrincipal>::new(ctx)?
            .fetch_view::<CountedAssignments>(&id)
            .await
    }

    async fn sample_counted_raw_item(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
    ) -> Result<Option<AssignmentView>> {
        Query::<SamplePrincipal>::new(ctx)?
            .fetch::<ViewProjector<CountedAssignments>>(&id)
            .await
    }

    async fn sample_counted_raw_item_json(
        &self,
        ctx: &Context<'_>,
        id: Uuid,
    ) -> Result<Option<Json<serde_json::Value>>> {
        Query::<SamplePrincipal>::new(ctx)?
            .fetch_json::<ViewProjector<CountedAssignments>>(&id)
            .await
    }
}

fn counted_slice() -> SliceFragment {
    SliceFragment::from_claims(
        "counted",
        [
            "sampleCountedWindow",
            "sampleCountedWindowJson",
            "sampleCountedItem",
            "sampleCountedRawItem",
            "sampleCountedRawItemJson",
        ]
        .map(str::to_string)
        .into(),
        vec!["AssignmentView".to_string()],
    )
}

pub async fn boot_counted_service(
    db: &TestDb,
    nats: Nats,
    pod: &str,
    window_capacity: usize,
) -> GraphqlService {
    boot_query_service(
        db,
        nats,
        base_config("se_counted", pod).with_window_capacity(window_capacity),
        CountedQueryRoot,
        counted_slice(),
        |engine| engine.register_view(CountedAssignments),
    )
    .await
}
