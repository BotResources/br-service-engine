use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::impact::Impact;
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::projector::Emission;
use service_engine::view::{Populate, Projector as ViewProjector, cohort_window};
use service_engine::{Cohort, CohortKey, Page, WindowSize};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::sample::assignment::{Assignment, AssignmentRow, AssignmentStore, AssignmentView};
use crate::sample::gated::AssignmentVisibility;
use crate::sample::principal::SamplePrincipal;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssignmentPage(Option<Page<Uuid>>);

impl AssignmentPage {
    pub fn head(size: WindowSize) -> Self {
        Self(Some(Page::head(size)))
    }

    pub fn before(before: Uuid, size: WindowSize) -> Self {
        Self(Some(Page::before(before, size)))
    }

    pub fn whole() -> Self {
        Self(None)
    }
}

async fn newest_keys(
    cx: &Populate<'_, SamplePrincipal>,
    page: Option<&Page<Uuid>>,
) -> Result<Vec<Uuid>, EngineError> {
    let rows = sqlx::query(
        "SELECT id FROM sample_assignment WHERE tenant_id = $1 AND ($2::uuid IS NULL OR id < $2) \
         ORDER BY id DESC LIMIT $3",
    )
    .bind(cx.principal().tenant())
    .bind(page.and_then(Page::cursor))
    .bind(page.map_or_else(|| cx.limit_all(), |page| cx.limit(page)))
    .fetch_all(cx.pool())
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
        let keys = newest_keys(cx, query.0.as_ref()).await?;
        Ok(match &query.0 {
            Some(page) => page.population(keys),
            None => Population::Ordered {
                keys,
                open_head: true,
            },
        })
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
