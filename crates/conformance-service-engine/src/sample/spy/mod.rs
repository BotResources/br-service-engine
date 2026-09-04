use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::impact::{Deps, Dims, ForeignKey, Impact};
use service_engine::name::{Namespace, NounName, ProjectorName};
use service_engine::population::{Interest, Inverse, Population, WindowQuery};
use service_engine::principal::Principal;
use service_engine::projector::{Emission, LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::wire::Noun;
use service_engine::{CohortKey, KeyBytes};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::sample::assignment::{Assignment, AssignmentFacts, AssignmentRow, AssignmentView};
mod modes;
mod recorder;

pub use modes::{CohortMode, WindowMode};
pub use recorder::Spy;

use crate::sample::gate::Gate;
use crate::sample::principal::SamplePrincipal;

pub const FOREIGN_NAMESPACE: &str = "identity.user";
pub const DEP_MEMBERSHIP: u8 = 0;

pub struct SpyAssignments {
    spy: Arc<Spy>,
    window: WindowMode,
    cohort: CohortMode,
    emission: Emission,
    dims: Dims,
    also: Option<NounName>,
    gate: Option<Arc<Gate>>,
    load_gate: Option<Arc<Gate>>,
    fail_switch: Option<Arc<AtomicBool>>,
    panic_switch: Option<Arc<AtomicBool>>,
    broken: bool,
}

impl SpyAssignments {
    pub const NAME: ProjectorName = ProjectorName::from_static("spy_assignments");
}

pub fn membership_dep() -> Deps {
    Deps::bit(DEP_MEMBERSHIP).expect("a declared dependency fits the bit set")
}

pub fn foreign_namespace() -> Namespace {
    Namespace::new(FOREIGN_NAMESPACE).expect("a valid namespace")
}

fn live_interest(dims: Dims, also: Option<NounName>) -> Interest {
    let interest = Interest::new()
        .on_noun(Assignment::NAME, dims)
        .on_foreign(foreign_namespace())
        .on_deps(membership_dep());
    match also {
        Some(noun) => interest.on_noun(noun, Dims::EMPTY),
        None => interest,
    }
}

impl Projector for SpyAssignments {
    type Principal = SamplePrincipal;
    type Key = Uuid;
    type Facts = AssignmentFacts;
    type View = AssignmentView;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[Assignment::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        pg: &'a PgPool,
        _window: &'a WindowParams,
        principal: &'a SamplePrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            self.spy.populates.fetch_add(1, Ordering::Relaxed);
            self.switched_off()?;
            if self.broken {
                return Err(EngineError::Service(
                    "the sample window cannot be assembled".into(),
                ));
            }
            let population = match self.window {
                WindowMode::Keys => {
                    let rows = sqlx::query("SELECT id FROM sample_assignment")
                        .fetch_all(pg)
                        .await?;
                    Population::Keys(rows.iter().map(|r| r.get::<Uuid, _>("id")).collect())
                }
                WindowMode::OrderedHead(limit) => {
                    let rows = sqlx::query(
                        "SELECT id FROM sample_assignment ORDER BY title DESC LIMIT $1",
                    )
                    .bind(limit)
                    .fetch_all(pg)
                    .await?;
                    Population::Ordered {
                        keys: rows.iter().map(|r| r.get::<Uuid, _>("id")).collect(),
                        open_head: true,
                    }
                }
                WindowMode::LiveQuery => Population::Query(WindowQuery::new(
                    live_interest(self.dims, self.also.clone()),
                    Arc::new(|_key: &Uuid, _impact: &Impact| true),
                )),
                WindowMode::QueryThenEmpty if self.spy.populates.load(Ordering::Relaxed) <= 1 => {
                    Population::Query(WindowQuery::new(
                        live_interest(self.dims, self.also.clone()),
                        Arc::new(|_key: &Uuid, _impact: &Impact| true),
                    ))
                }
                WindowMode::QueryThenEmpty => Population::Keys(BTreeSet::new()),
                WindowMode::MembershipQuery | WindowMode::MembershipOnlyQuery => {
                    let rows = sqlx::query("SELECT id FROM sample_assignment WHERE tenant_id = $1")
                        .bind(principal.tenant())
                        .fetch_all(pg)
                        .await?;
                    Population::Query(
                        WindowQuery::new(
                            live_interest(self.dims, self.also.clone()),
                            Arc::new(|_key: &Uuid, _impact: &Impact| false),
                        )
                        .with_keys(rows.iter().map(|r| r.get::<Uuid, _>("id"))),
                    )
                }
            };
            if let Some(gate) = &self.gate {
                gate.pass().await;
            }
            Ok(population)
        })
    }

    fn inverse(&self, foreign: &ForeignKey) -> Inverse<Uuid> {
        if foreign.namespace() != &foreign_namespace() {
            return Inverse::None;
        }
        match Uuid::parse_str(foreign.key().as_str()) {
            Ok(id) => Inverse::Keys(BTreeSet::from([id])),
            Err(_) => Inverse::None,
        }
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, SamplePrincipal>,
    ) -> BoxFuture<'a, Result<AssignmentFacts, EngineError>> {
        Box::pin(async move {
            self.spy.loads.fetch_add(1, Ordering::Relaxed);
            self.switched_off()?;
            let sql = "SELECT id, tenant_id, title, closed FROM sample_assignment \
                       WHERE id = ANY($1)";
            let rows = match scope {
                LoadScope::Bulk { pg, keys, .. } => {
                    sqlx::query(sql).bind(keys).fetch_all(pg).await?
                }
                LoadScope::PerPrincipal { conn, keys, .. } => {
                    sqlx::query(sql).bind(keys).fetch_all(&mut *conn).await?
                }
            };
            let rows: BTreeMap<Uuid, AssignmentRow> = rows
                .into_iter()
                .map(|row| {
                    let id: Uuid = row.get("id");
                    (
                        id,
                        AssignmentRow {
                            id,
                            tenant_id: row.get("tenant_id"),
                            title: row.get("title"),
                            closed: row.get("closed"),
                        },
                    )
                })
                .collect();
            self.spy
                .loaded
                .lock()
                .unwrap()
                .push(rows.keys().copied().collect());
            if let Some(gate) = &self.load_gate {
                gate.pass().await;
            }
            Ok(AssignmentFacts { rows })
        })
    }

    fn project(
        &self,
        facts: &AssignmentFacts,
        key: &Uuid,
        principal: &SamplePrincipal,
    ) -> Option<AssignmentView> {
        self.spy.projects.fetch_add(1, Ordering::Relaxed);
        if self
            .panic_switch
            .as_ref()
            .is_some_and(|switch| switch.load(Ordering::Relaxed))
        {
            panic!("the sample projector was switched to panic for the duration of the test");
        }
        let row = facts.rows.get(key)?;
        if self.window != WindowMode::MembershipOnlyQuery && row.tenant_id != principal.tenant() {
            return None;
        }
        Some(AssignmentView {
            id: row.id,
            title: row.title.clone(),
            closed: row.closed,
            can_close: !row.closed,
        })
    }

    fn cohort(&self, principal: &SamplePrincipal) -> CohortKey {
        match self.cohort {
            CohortMode::PerPrincipal => CohortKey::principal(principal.id()),
            CohortMode::PerTenant => CohortKey::of(&[principal.tenant()]),
        }
    }

    fn emission(&self, _impact: &Impact) -> Emission {
        self.emission
    }
}

pub fn assignment_key(id: Uuid) -> KeyBytes {
    KeyBytes::encode(&id).expect("an assignment key encodes")
}
