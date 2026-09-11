use std::collections::BTreeSet;

use br_core_auth::{AuthMethod, Passport, PassportClaims};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::EngineError;
use crate::impact::ForeignKey;
use crate::name::{NounName, ProjectorName};
use crate::population::{Inverse, Population};
use crate::principal::{Principal, PrincipalId};
use crate::projector::{LoadScope, Projector};
use crate::session::WindowParams;
use crate::wire::Noun;

pub struct Assignment;

impl Noun for Assignment {
    type Key = Uuid;
    const NAME: NounName = NounName::from_static("assignment");
}

#[derive(Debug, Clone)]
pub struct TestPrincipal {
    id: PrincipalId,
    passport: Passport,
}

impl TestPrincipal {
    pub fn new() -> Self {
        let user = Uuid::now_v7();
        Self {
            id: PrincipalId::from(user),
            passport: Passport::human(
                user,
                false,
                true,
                AuthMethod::Jwt,
                None,
                PassportClaims::new(),
            ),
        }
    }
}

impl Principal for TestPrincipal {
    fn id(&self) -> PrincipalId {
        self.id
    }

    fn passport(&self) -> &Passport {
        &self.passport
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Title(pub String);

macro_rules! stub_projector {
    ($ty:ident, $name:literal, $key:ty) => {
        pub struct $ty;

        impl Projector for $ty {
            type Principal = TestPrincipal;
            type Key = $key;
            type Facts = ();
            type View = Title;

            fn name(&self) -> ProjectorName {
                ProjectorName::from_static($name)
            }

            fn nouns(&self) -> &'static [NounName] {
                const NOUNS: &[NounName] = &[Assignment::NAME];
                NOUNS
            }

            fn populate<'a>(
                &'a self,
                _pg: &'a PgPool,
                _window: &'a WindowParams,
                _principal: &'a TestPrincipal,
            ) -> BoxFuture<'a, Result<Population<$key>, EngineError>> {
                Box::pin(async move { Ok(Population::Keys(BTreeSet::new())) })
            }

            fn inverse(&self, _foreign: &ForeignKey) -> Inverse<$key> {
                Inverse::None
            }

            fn load<'a>(
                &'a self,
                _scope: LoadScope<'a, $key, TestPrincipal>,
            ) -> BoxFuture<'a, Result<(), EngineError>> {
                Box::pin(async move { Ok(()) })
            }

            fn project(
                &self,
                _facts: &(),
                _key: &$key,
                _principal: &TestPrincipal,
            ) -> Option<Title> {
                None
            }
        }
    };
}

stub_projector!(AssignmentKeyProjector, "assignments", Uuid);
stub_projector!(TwinProjector, "assignment_titles", Uuid);
stub_projector!(MiskeyedProjector, "miskeyed", String);
