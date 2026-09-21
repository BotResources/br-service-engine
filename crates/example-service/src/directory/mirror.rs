use example_contract::PERSON_PREFIX;
use example_lib_roster::{KNOWN_PERSON_NAMESPACE, KNOWN_PERSONS_TABLE};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::mirror::{
    Change, Column, KnownRow, Mirror, MirrorReady, Project, Projection, col,
};
use service_engine::name::MirrorName;
use service_engine::nats::KvKey;
use service_engine::{Consumed, Shadows};
use sqlx::PgPool;
use uuid::Uuid;

pub const DIRECTORY_MIRROR: MirrorName = MirrorName::from_static("directory");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumedPerson {
    pub id: Uuid,
    pub email: String,
    pub display_name: String,
}

impl Consumed for ConsumedPerson {
    const PREFIX: &'static str = PERSON_PREFIX;
}

fn person_key(id: Uuid) -> KvKey {
    KvKey::new(format!("{PERSON_PREFIX}{id}")).expect("a published-person key is valid")
}

fn id_of(key: &KvKey) -> Option<Uuid> {
    key.as_str()
        .strip_prefix(PERSON_PREFIX)
        .and_then(|raw| Uuid::parse_str(raw).ok())
}

struct KnownPersonRow {
    id: Uuid,
    email: String,
    display_name: String,
}

impl KnownRow for KnownPersonRow {
    const TABLE: &'static str = KNOWN_PERSONS_TABLE;
    const NAMESPACE: &'static str = KNOWN_PERSON_NAMESPACE;

    fn key(&self) -> Vec<Column> {
        vec![col("user_id", self.id)]
    }

    fn values(&self) -> Vec<Column> {
        vec![
            col("email", self.email.clone()),
            col("display_name", self.display_name.clone()),
        ]
    }
}

struct DirectoryProjection;

impl Project<Uuid> for DirectoryProjection {
    type Error = EngineError;

    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        id: Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let person = cx.shadow::<ConsumedPerson>().get(&person_key(id)).cloned();
            match person {
                Some(person) => cx
                    .upsert(KnownPersonRow {
                        id,
                        email: person.email,
                        display_name: person.display_name,
                    })
                    .await
                    .map(|_| ()),
                None => cx.retire::<KnownPersonRow>(vec![col("user_id", id)]).await,
            }
        })
    }
}

pub fn directory_mirror() -> MirrorReady<Uuid, impl Project<Uuid>> {
    Mirror::new(DIRECTORY_MIRROR)
        .consume::<ConsumedPerson>()
        .keyed_by(|_shadows: &Shadows, change: &Change| {
            id_of(&change.key).into_iter().collect::<Vec<Uuid>>()
        })
        .project(DirectoryProjection)
        .reconcile_keys(|pool: PgPool| {
            Box::pin(async move {
                let ids: Vec<Uuid> = sqlx::query_scalar("SELECT user_id FROM roster.known_persons")
                    .fetch_all(&pool)
                    .await?;
                Ok(ids)
            }) as BoxFuture<'static, Result<Vec<Uuid>, EngineError>>
        })
}
