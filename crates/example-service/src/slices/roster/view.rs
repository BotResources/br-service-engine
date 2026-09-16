use std::collections::{BTreeMap, BTreeSet};

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::impact::ForeignKey;
use service_engine::name::{NounName, ProjectorName};
use service_engine::population::{Inverse, Population};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::wire::Noun;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::mirror::PERSON_NAMESPACE;
use crate::kernel::AppPrincipal;

pub struct KnownPerson;

impl service_engine::wire::Noun for KnownPerson {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("known_person");
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct RosterView {
    pub user_id: Uuid,
    pub email: String,
    pub display_name: String,
}

#[derive(Default)]
pub struct RosterUsers;

impl RosterUsers {
    pub const NAME: ProjectorName = ProjectorName::from_static("roster_users");
}

impl Projector for RosterUsers {
    type Principal = AppPrincipal;
    type Key = Uuid;
    type Facts = BTreeMap<Uuid, (String, String)>;
    type View = RosterView;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[KnownPerson::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        pg: &'a PgPool,
        _window: &'a WindowParams,
        _principal: &'a AppPrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query("SELECT user_id FROM known_persons")
                .fetch_all(pg)
                .await?;
            Ok(Population::Keys(
                rows.iter()
                    .map(|row| row.get::<Uuid, _>("user_id"))
                    .collect(),
            ))
        })
    }

    fn inverse(&self, foreign: &ForeignKey) -> Inverse<Uuid> {
        if foreign.namespace().as_str() != PERSON_NAMESPACE {
            return Inverse::None;
        }
        match Uuid::parse_str(foreign.key().as_str()) {
            Ok(id) => Inverse::Keys(BTreeSet::from([id])),
            Err(_) => Inverse::None,
        }
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, AppPrincipal>,
    ) -> BoxFuture<'a, Result<BTreeMap<Uuid, (String, String)>, EngineError>> {
        Box::pin(async move {
            let keys = scope.keys().to_vec();
            let sql =
                "SELECT user_id, email, display_name FROM known_persons WHERE user_id = ANY($1)";
            let rows = match scope {
                LoadScope::Bulk { pg, .. } => sqlx::query(sql).bind(&keys).fetch_all(pg).await?,
                LoadScope::PerPrincipal { conn, .. } => {
                    sqlx::query(sql).bind(&keys).fetch_all(&mut *conn).await?
                }
            };
            Ok(rows
                .into_iter()
                .map(|row| {
                    (
                        row.get::<Uuid, _>("user_id"),
                        (
                            row.get::<String, _>("email"),
                            row.get::<String, _>("display_name"),
                        ),
                    )
                })
                .collect())
        })
    }

    fn project(
        &self,
        facts: &BTreeMap<Uuid, (String, String)>,
        key: &Uuid,
        _principal: &AppPrincipal,
    ) -> Result<Option<RosterView>, EngineError> {
        Ok(facts.get(key).map(|(email, display_name)| RosterView {
            user_id: *key,
            email: email.clone(),
            display_name: display_name.clone(),
        }))
    }
}
