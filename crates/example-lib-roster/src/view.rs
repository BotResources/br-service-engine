use std::collections::{BTreeMap, BTreeSet};

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::KeyCeiling;
use service_engine::error::EngineError;
use service_engine::impact::ForeignKey;
use service_engine::name::{NounName, ProjectorName};
use service_engine::population::{Inverse, Population};
use service_engine::projector::{LoadScope, Projector};
use service_engine::session::WindowParams;
use service_engine::wire::Noun;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::KNOWN_PERSON_NAMESPACE;
use crate::principal::RosterPrincipal;

pub struct KnownPerson;

impl Noun for KnownPerson {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("known_person");
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, async_graphql::SimpleObject)]
pub struct RosterView {
    pub user_id: Uuid,
    pub email: String,
    pub display_name: String,
}

pub struct RosterUsers<P>(std::marker::PhantomData<fn() -> P>);

impl<P> Default for RosterUsers<P> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<P> RosterUsers<P> {
    pub const NAME: ProjectorName = ProjectorName::from_static("roster_users");
}

fn invalidated(foreign: &ForeignKey) -> Inverse<Uuid> {
    if foreign.namespace().as_str() != KNOWN_PERSON_NAMESPACE {
        return Inverse::None;
    }
    match Uuid::parse_str(foreign.key().as_str()) {
        Ok(id) => Inverse::Keys(BTreeSet::from([id])),
        Err(_) => Inverse::None,
    }
}

impl<P: RosterPrincipal> Projector for RosterUsers<P> {
    type Principal = P;
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
        _ceiling: KeyCeiling,
        _principal: &'a P,
    ) -> BoxFuture<'a, Result<Population<Uuid>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query("SELECT user_id FROM roster.known_persons")
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
        invalidated(foreign)
    }

    fn load<'a>(
        &'a self,
        scope: LoadScope<'a, Uuid, P>,
    ) -> BoxFuture<'a, Result<BTreeMap<Uuid, (String, String)>, EngineError>> {
        Box::pin(async move {
            let keys = scope.keys().to_vec();
            let sql = "SELECT user_id, email, display_name FROM roster.known_persons \
                       WHERE user_id = ANY($1)";
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
        _principal: &P,
    ) -> Result<Option<RosterView>, EngineError> {
        Ok(facts.get(key).map(|(email, display_name)| RosterView {
            user_id: *key,
            email: email.clone(),
            display_name: display_name.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_foreign_key_in_the_roster_namespace_invalidates_its_uuid() {
        let id = Uuid::now_v7();
        let foreign = ForeignKey::new(KNOWN_PERSON_NAMESPACE, &id.to_string()).unwrap();
        match invalidated(&foreign) {
            Inverse::Keys(keys) => assert_eq!(keys, BTreeSet::from([id])),
            _ => panic!("a roster-namespace key must invalidate its own uuid"),
        }
    }

    #[test]
    fn a_foreign_key_in_another_namespace_invalidates_nothing() {
        let foreign = ForeignKey::new("other.person", &Uuid::now_v7().to_string()).unwrap();
        assert!(matches!(invalidated(&foreign), Inverse::None));
    }

    #[test]
    fn a_roster_key_that_is_not_a_uuid_invalidates_nothing() {
        let foreign = ForeignKey::new(KNOWN_PERSON_NAMESPACE, "not-a-uuid").unwrap();
        assert!(matches!(invalidated(&foreign), Inverse::None));
    }

    #[test]
    fn a_view_survives_a_json_round_trip() {
        let view = RosterView {
            user_id: Uuid::now_v7(),
            email: "a@example.test".to_string(),
            display_name: "A".to_string(),
        };
        let json = serde_json::to_string(&view).unwrap();
        let back: RosterView = serde_json::from_str(&json).unwrap();
        assert_eq!(view, back);
    }
}
