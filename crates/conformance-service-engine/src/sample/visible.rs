use service_engine::error::EngineError;
use service_engine::impact::Deps;
use service_engine::name::ProjectorName;
use service_engine::population::Population;
use service_engine::view::{Populate, Projector, cohort_window};
use service_engine::visibility::{Cohorts, Visibility};
use uuid::Uuid;

use crate::sample::assignment::{Assignment, AssignmentRow, AssignmentStore, AssignmentView};
use crate::sample::gated::AssignmentVisibility;
use crate::sample::principal::SamplePrincipal;

pub struct SnapshotAssignmentVisibility;

impl Visibility for SnapshotAssignmentVisibility {
    type Row = AssignmentRow;
    type Principal = SamplePrincipal;

    const DEPS: Deps = AssignmentVisibility::DEPS;

    const LIVE: bool = false;

    fn cohorts(row: &AssignmentRow) -> Cohorts {
        AssignmentVisibility::cohorts(row)
    }

    fn memberships(principal: &SamplePrincipal) -> Cohorts {
        AssignmentVisibility::memberships(principal)
    }
}

#[derive(Default)]
pub struct VisibleAssignments;

impl VisibleAssignments {
    pub const NAME: ProjectorName = ProjectorName::from_static("visible_assignments");
}

impl Projector for VisibleAssignments {
    type Principal = SamplePrincipal;
    type Noun = Assignment;
    type Store = AssignmentStore;
    type Query = ();
    type Out = AssignmentView;
    type Visibility = SnapshotAssignmentVisibility;

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
        Ok(AssignmentView {
            id: row.id,
            title: row.title.clone(),
            closed: row.closed,
            can_close: !row.closed,
        })
    }
}
