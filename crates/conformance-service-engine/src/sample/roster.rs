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

use crate::sample::principal::SamplePrincipal;

pub struct KnownUserNoun;

impl Noun for KnownUserNoun {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("known_user");
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RosterUserView {
    pub user_id: Uuid,
    pub email: String,
}

pub struct RosterUsers;

impl RosterUsers {
    pub const NAME: ProjectorName = ProjectorName::from_static("roster_users");
}

impl Projector for RosterUsers {
    type Principal = SamplePrincipal;
    type Key = Uuid;
    type Facts = BTreeMap<Uuid, String>;
    type View = RosterUserView;

    fn name(&self) -> ProjectorName {
        Self::NAME
    }

    fn nouns(&self) -> &'static [NounName] {
        const NOUNS: &[NounName] = &[KnownUserNoun::NAME];
        NOUNS
    }

    fn populate<'a>(
        &'a self,
        pg: &'a PgPool,
        _window: &'a WindowParams,
        _principal: &'a SamplePrincipal,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query("SELECT user_id FROM known_users")
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
        if foreign.namespace().as_str() != "identity.user" {
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
    ) -> BoxFuture<'a, Result<BTreeMap<Uuid, String>, EngineError>> {
        Box::pin(async move {
            let sql = "SELECT user_id, email FROM known_users WHERE user_id = ANY($1)";
            let rows = match scope {
                LoadScope::Bulk { pg, keys, .. } => {
                    sqlx::query(sql).bind(keys).fetch_all(pg).await?
                }
                LoadScope::PerPrincipal { conn, keys, .. } => {
                    sqlx::query(sql).bind(keys).fetch_all(&mut *conn).await?
                }
            };
            Ok(rows
                .into_iter()
                .map(|row| (row.get::<Uuid, _>("user_id"), row.get::<String, _>("email")))
                .collect())
        })
    }

    fn project(
        &self,
        facts: &BTreeMap<Uuid, String>,
        key: &Uuid,
        _principal: &SamplePrincipal,
    ) -> Option<RosterUserView> {
        facts.get(key).map(|email| RosterUserView {
            user_id: *key,
            email: email.clone(),
        })
    }
}
