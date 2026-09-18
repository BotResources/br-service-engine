use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::impact::Impact;
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::projector::Emission;
use service_engine::view::{Populate, Projector as ViewProjector, cohort_window};
use service_engine::{Cohort, CohortKey};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::sample::assignment::{Assignment, AssignmentRow, AssignmentStore, AssignmentView};
use crate::sample::gated::AssignmentVisibility;
use crate::sample::principal::SamplePrincipal;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssignmentPage {
    pub before: Option<Uuid>,
    pub size: Option<i64>,
}

impl AssignmentPage {
    pub fn head(size: i64) -> Self {
        Self {
            before: None,
            size: Some(size),
        }
    }

    pub fn before(before: Uuid, size: i64) -> Self {
        Self {
            before: Some(before),
            size: Some(size),
        }
    }
}

async fn head_keys(pg: &PgPool, tenant: Uuid, size: i64) -> Result<BTreeSet<Uuid>, EngineError> {
    let rows = sqlx::query(
        "SELECT id FROM sample_assignment WHERE tenant_id = $1 ORDER BY id DESC LIMIT $2",
    )
    .bind(tenant)
    .bind(size)
    .fetch_all(pg)
    .await?;
    Ok(rows.iter().map(|r| r.get::<Uuid, _>("id")).collect())
}

async fn page_keys(
    pg: &PgPool,
    tenant: Uuid,
    before: Uuid,
    size: i64,
) -> Result<BTreeSet<Uuid>, EngineError> {
    let rows = sqlx::query(
        "SELECT id FROM sample_assignment WHERE tenant_id = $1 AND id < $2 \
         ORDER BY id DESC LIMIT $3",
    )
    .bind(tenant)
    .bind(before)
    .bind(size)
    .fetch_all(pg)
    .await?;
    Ok(rows.iter().map(|r| r.get::<Uuid, _>("id")).collect())
}

async fn tenant_keys(pg: &PgPool, tenant: Uuid) -> Result<BTreeSet<Uuid>, EngineError> {
    let rows = sqlx::query("SELECT id FROM sample_assignment WHERE tenant_id = $1")
        .bind(tenant)
        .fetch_all(pg)
        .await?;
    Ok(rows.iter().map(|r| r.get::<Uuid, _>("id")).collect())
}

fn view(row: &AssignmentRow) -> AssignmentView {
    AssignmentView {
        id: row.id,
        title: row.title.clone(),
        closed: row.closed,
        can_close: !row.closed,
    }
}

#[derive(Default)]
pub struct PagedAssignments;

impl PagedAssignments {
    pub const NAME: ProjectorName = ProjectorName::from_static("paged_assignments");
}

impl ViewProjector for PagedAssignments {
    type Principal = SamplePrincipal;
    type Noun = Assignment;
    type Store = AssignmentStore;
    type Query = AssignmentPage;
    type Out = AssignmentView;
    type Visibility = AssignmentVisibility;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, SamplePrincipal>,
        query: &AssignmentPage,
    ) -> Result<Population<Uuid>, EngineError> {
        let tenant = cx.principal().tenant();
        let keys = match (query.before, query.size) {
            (Some(before), Some(size)) => page_keys(cx.pool(), tenant, before, size).await?,
            (None, Some(size)) => head_keys(cx.pool(), tenant, size).await?,
            _ => tenant_keys(cx.pool(), tenant).await?,
        };
        Ok(Population::Keys(keys))
    }

    fn project(
        row: &AssignmentRow,
        _principal: &SamplePrincipal,
    ) -> Result<AssignmentView, EngineError> {
        Ok(view(row))
    }
}

#[derive(Default)]
pub struct CohortAssignments;

impl CohortAssignments {
    pub const NAME: ProjectorName = ProjectorName::from_static("cohort_assignments");
}

impl ViewProjector for CohortAssignments {
    type Principal = SamplePrincipal;
    type Noun = Assignment;
    type Store = AssignmentStore;
    type Query = ();
    type Out = AssignmentView;
    type Visibility = AssignmentVisibility;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, SamplePrincipal>,
        _query: &(),
    ) -> Result<Population<Uuid>, EngineError> {
        Ok(Population::Keys(
            tenant_keys(cx.pool(), cx.principal().tenant()).await?,
        ))
    }

    fn project(
        row: &AssignmentRow,
        _principal: &SamplePrincipal,
    ) -> Result<AssignmentView, EngineError> {
        Ok(view(row))
    }

    fn cohort(principal: &SamplePrincipal) -> CohortKey {
        Cohort::uuid("tenant", principal.tenant()).key()
    }
}

#[derive(Default)]
pub struct CohortIndexedAssignments;

impl CohortIndexedAssignments {
    pub const NAME: ProjectorName = ProjectorName::from_static("cohort_indexed_assignments");
}

impl ViewProjector for CohortIndexedAssignments {
    type Principal = SamplePrincipal;
    type Noun = Assignment;
    type Store = AssignmentStore;
    type Query = ();
    type Out = AssignmentView;
    type Visibility = AssignmentVisibility;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, SamplePrincipal>,
        _query: &(),
    ) -> Result<Population<Uuid>, EngineError> {
        cohort_window::<Self>(cx).await
    }

    fn project(
        row: &AssignmentRow,
        _principal: &SamplePrincipal,
    ) -> Result<AssignmentView, EngineError> {
        Ok(view(row))
    }
}

#[derive(Default)]
pub struct ThresholdAssignments;

impl ThresholdAssignments {
    pub const NAME: ProjectorName = ProjectorName::from_static("threshold_assignments");
}

impl ViewProjector for ThresholdAssignments {
    type Principal = SamplePrincipal;
    type Noun = Assignment;
    type Store = AssignmentStore;
    type Query = ();
    type Out = AssignmentView;
    type Visibility = AssignmentVisibility;

    const NAME: ProjectorName = Self::NAME;

    const RESET_THRESHOLD: Option<usize> = Some(1);

    async fn populate(
        cx: &Populate<'_, SamplePrincipal>,
        _query: &(),
    ) -> Result<Population<Uuid>, EngineError> {
        Ok(Population::Keys(
            tenant_keys(cx.pool(), cx.principal().tenant()).await?,
        ))
    }

    fn project(
        row: &AssignmentRow,
        _principal: &SamplePrincipal,
    ) -> Result<AssignmentView, EngineError> {
        Ok(view(row))
    }
}

#[derive(Default)]
pub struct PerImpactAssignments;

impl PerImpactAssignments {
    pub const NAME: ProjectorName = ProjectorName::from_static("per_impact_assignments");
}

impl ViewProjector for PerImpactAssignments {
    type Principal = SamplePrincipal;
    type Noun = Assignment;
    type Store = AssignmentStore;
    type Query = ();
    type Out = AssignmentView;
    type Visibility = AssignmentVisibility;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(
        cx: &Populate<'_, SamplePrincipal>,
        _query: &(),
    ) -> Result<Population<Uuid>, EngineError> {
        Ok(Population::Keys(
            tenant_keys(cx.pool(), cx.principal().tenant()).await?,
        ))
    }

    fn project(
        row: &AssignmentRow,
        _principal: &SamplePrincipal,
    ) -> Result<AssignmentView, EngineError> {
        Ok(view(row))
    }

    fn emission(_impact: &Impact) -> Emission {
        Emission::PerImpact
    }
}
