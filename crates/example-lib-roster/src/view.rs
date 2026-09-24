use std::collections::BTreeSet;

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::impact::ForeignKey;
use service_engine::name::{NounName, ProjectorName};
use service_engine::persistence::{Persistence, PersistenceStyle};
use service_engine::population::{Inverse, Population};
use service_engine::view::{Populate, Projector};
use service_engine::visibility::Unrestricted;
use service_engine::wire::Noun;
use sqlx::{PgConnection, Row};
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

service_engine::open_access!(
    pub RosterIsOpen = "the roster lists every known person to every principal of the host"
);

pub struct RosterStore;

impl Persistence for RosterStore {
    type Aggregate = RosterView;
    type Key = Uuid;
    type Event = ();

    const STYLE: PersistenceStyle = PersistenceStyle::Crud;

    fn read_many<'a>(
        conn: &'a mut PgConnection,
        keys: &'a [Uuid],
    ) -> BoxFuture<'a, Result<Vec<(Uuid, RosterView)>, EngineError>> {
        Box::pin(async move {
            let rows = sqlx::query(
                "SELECT user_id, email, display_name FROM roster.known_persons \
                 WHERE user_id = ANY($1)",
            )
            .bind(keys)
            .fetch_all(conn)
            .await?;
            Ok(rows
                .iter()
                .map(|row| {
                    let person = RosterView {
                        user_id: row.get("user_id"),
                        email: row.get("email"),
                        display_name: row.get("display_name"),
                    };
                    (person.user_id, person)
                })
                .collect())
        })
    }

    fn save<'a>(
        _conn: &'a mut PgConnection,
        _person: &'a RosterView,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Err(mirrored_only()) })
    }

    fn create<'a>(
        _conn: &'a mut PgConnection,
        _person: &'a RosterView,
        _events: &'a [()],
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async { Err(mirrored_only()) })
    }
}

fn mirrored_only() -> EngineError {
    EngineError::Service(
        "roster.known_persons is written by the host's directory mirror, never by a mutation"
            .into(),
    )
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
    type Noun = KnownPerson;
    type Store = RosterStore;
    type Query = ();
    type Out = RosterView;
    type Visibility = Unrestricted<RosterView, P, RosterIsOpen>;

    const NAME: ProjectorName = Self::NAME;

    async fn populate(cx: &Populate<'_, P>, _query: &()) -> Result<Population<Uuid>, EngineError> {
        let rows = sqlx::query("SELECT user_id FROM roster.known_persons LIMIT $1")
            .bind(cx.limit_all())
            .fetch_all(cx.pool())
            .await?;
        Ok(Population::Keys(
            rows.iter()
                .map(|row| row.get::<Uuid, _>("user_id"))
                .collect(),
        ))
    }

    fn project(person: &RosterView, _principal: &P) -> Result<RosterView, EngineError> {
        Ok(person.clone())
    }

    fn inverse(foreign: &ForeignKey) -> Inverse<Uuid> {
        invalidated(foreign)
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
