use std::collections::BTreeSet;
use std::sync::{Mutex, MutexGuard, PoisonError};

use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::name::ProjectorName;
use service_engine::persistence::{CohortIndex, Persistence, PersistenceStyle};
use service_engine::population::Population;
use service_engine::view::{Populate, Projector, cohort_window};
use service_engine::{Cohort, KeyCeiling};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::sample::assignment::{Assignment, AssignmentRow, AssignmentStore, AssignmentView};
use crate::sample::gated::AssignmentVisibility;
use crate::sample::principal::SamplePrincipal;

static ASKED: Mutex<Vec<(Vec<Uuid>, KeyCeiling)>> = Mutex::new(Vec::new());
static READ: Mutex<BTreeSet<Uuid>> = Mutex::new(BTreeSet::new());

fn held<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

pub fn ceilings_asked_for(tenant: Uuid) -> Vec<usize> {
    held(&ASKED)
        .iter()
        .filter(|(tenants, _)| tenants == &[tenant])
        .map(|(_, ceiling)| ceiling.get())
        .collect()
}

pub fn rows_read_of(ids: &[Uuid]) -> usize {
    let read = held(&READ);
    ids.iter().filter(|id| read.contains(id)).count()
}

pub struct CountedStore;

impl Persistence for CountedStore {
    type Aggregate = AssignmentRow;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, AssignmentRow)>, EngineError>> {
        held(&READ).extend(keys.iter().copied());
        AssignmentStore::read_many(conn, keys)
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a AssignmentRow,
        events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        AssignmentStore::save(conn, aggregate, events)
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a AssignmentRow,
        events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        AssignmentStore::create(conn, aggregate, events)
    }
}

impl CohortIndex for CountedStore {
    fn keys_in_cohorts<'a>(
        conn: &'a mut PgConnection,
        cohorts: &'a [Cohort],
        ceiling: KeyCeiling,
    ) -> BoxFuture<'a, Result<Vec<Uuid>, EngineError>> {
        held(&ASKED).push((Cohort::uuids(cohorts, "tenant"), ceiling));
        AssignmentStore::keys_in_cohorts(conn, cohorts, ceiling)
    }
}

#[derive(Default)]
pub struct CountedAssignments;

impl CountedAssignments {
    pub const NAME: ProjectorName = ProjectorName::from_static("counted_assignments");
}

impl Projector for CountedAssignments {
    type Principal = SamplePrincipal;
    type Noun = Assignment;
    type Store = CountedStore;
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
        Ok(AssignmentView {
            id: row.id,
            title: row.title.clone(),
            closed: row.closed,
            can_close: !row.closed,
        })
    }
}
