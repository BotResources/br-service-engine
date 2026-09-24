use std::collections::{BTreeMap, BTreeSet};

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::KeyCeiling;
use service_engine::error::EngineError;
use service_engine::gate::{Affordances, Gate, Gated, Reason};
use service_engine::impact::ForeignKey;
use service_engine::name::{NounName, ProjectorName};
use service_engine::persistence::{Aggregate, Persistence, PersistenceStyle};
use service_engine::population::{Inverse, Population};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::wire::Noun;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::sample::principal::SamplePrincipal;

pub mod reasons {
    use service_engine::gate::Reason;

    pub const ALREADY_CLOSED: Reason = Reason::new("ALREADY_CLOSED");
}

pub struct Widget;

impl Noun for Widget {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("widget");
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WidgetRow {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub label: String,
    pub closed: bool,
}

service_engine::gated! {
    WidgetRow, SamplePrincipal;
    "close" => fn close_gate(this, _principal) {
        if this.closed {
            Gate::blocked(reasons::ALREADY_CLOSED)
        } else {
            Gate::allowed()
        }
    }
}

impl WidgetRow {
    pub fn close(&mut self, principal: &SamplePrincipal) -> Result<(), Reason> {
        self.close_gate(principal).require()?;
        self.closed = true;
        Ok(())
    }
}

pub struct WidgetStore;

impl Persistence for WidgetStore {
    type Aggregate = WidgetRow;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, WidgetRow)>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(
                "SELECT id, tenant_id, label, closed FROM sample_widget WHERE id = ANY($1)",
            )
            .bind(keys)
            .fetch_all(conn)
            .await?;
            Ok(rows
                .into_iter()
                .map(|row| {
                    let widget = WidgetRow {
                        id: row.get("id"),
                        tenant_id: row.get("tenant_id"),
                        label: row.get("label"),
                        closed: row.get("closed"),
                    };
                    (widget.id, widget)
                })
                .collect())
        })
    }

    fn save<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a WidgetRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO sample_widget (id, tenant_id, label, closed) VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id, \
                   label = EXCLUDED.label, closed = EXCLUDED.closed",
            )
            .bind(aggregate.id)
            .bind(aggregate.tenant_id)
            .bind(&aggregate.label)
            .bind(aggregate.closed)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn create<'a>(
        conn: &'a mut PgConnection,
        aggregate: &'a WidgetRow,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO sample_widget (id, tenant_id, label, closed) VALUES ($1, $2, $3, $4)",
            )
            .bind(aggregate.id)
            .bind(aggregate.tenant_id)
            .bind(&aggregate.label)
            .bind(aggregate.closed)
            .execute(conn)
            .await?;
            Ok(())
        })
    }

    fn delete<'a>(
        conn: &'a mut PgConnection,
        key: &'a Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("DELETE FROM sample_widget WHERE id = $1")
                .bind(key)
                .execute(conn)
                .await?;
            Ok(())
        })
    }
}

impl Aggregate for WidgetRow {
    type Store = WidgetStore;

    fn key(&self) -> Uuid {
        self.id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WidgetFacts {
    pub rows: BTreeMap<Uuid, WidgetRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct WidgetView {
    pub id: Uuid,
    pub label: String,
    pub closed: bool,
    pub affordances: Affordances,
}

#[derive(Default)]
pub struct WidgetProjector;

impl WidgetProjector {
    pub const NAME: ProjectorName = ProjectorName::from_static("widgets");
}

impl Projector for WidgetProjector {
    type Principal = SamplePrincipal;
    type Key = Uuid;
    type Facts = WidgetFacts;
    type View = WidgetView;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[Widget::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        pg: &'a sqlx::PgPool,
        _window: &'a WindowParams,
        _ceiling: KeyCeiling,
        principal: &'a SamplePrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query("SELECT id FROM sample_widget WHERE tenant_id = $1")
                .bind(principal.tenant())
                .fetch_all(pg)
                .await?;
            Ok(Population::Keys(
                rows.iter()
                    .map(|row| row.get::<Uuid, _>("id"))
                    .collect::<BTreeSet<_>>(),
            ))
        })
    }

    fn inverse(&self, _foreign: &ForeignKey) -> Inverse<Uuid> {
        Inverse::None
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, SamplePrincipal>,
    ) -> BoxFuture<'a, Result<WidgetFacts, EngineError>> {
        Box::pin(async move {
            let sql = "SELECT id, tenant_id, label, closed FROM sample_widget WHERE id = ANY($1)";
            let rows = match scope {
                LoadScope::Bulk { pg, keys, .. } => {
                    sqlx::query(sql).bind(keys).fetch_all(pg).await?
                }
                LoadScope::PerPrincipal { conn, keys, .. } => {
                    sqlx::query(sql).bind(keys).fetch_all(&mut *conn).await?
                }
            };
            Ok(WidgetFacts {
                rows: rows
                    .into_iter()
                    .map(|row| {
                        let id: Uuid = row.get("id");
                        (
                            id,
                            WidgetRow {
                                id,
                                tenant_id: row.get("tenant_id"),
                                label: row.get("label"),
                                closed: row.get("closed"),
                            },
                        )
                    })
                    .collect(),
            })
        })
    }

    fn project(
        &self,
        facts: &WidgetFacts,
        key: &Uuid,
        principal: &SamplePrincipal,
    ) -> Result<Option<WidgetView>, EngineError> {
        let Some(row) = facts.rows.get(key) else {
            return Ok(None);
        };
        Ok(Some(WidgetView {
            id: row.id,
            label: row.label.clone(),
            closed: row.closed,
            affordances: row.affordances(principal),
        }))
    }
}
