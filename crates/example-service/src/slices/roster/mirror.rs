use example_contract::PERSON_PREFIX;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::mirror::{Change, Known, KnownScope, Mirror, MirrorReady, Project, Projection};
use service_engine::name::MirrorName;
use service_engine::nats::KvKey;
use service_engine::{Consumed, Shadows};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

pub const DIRECTORY_MIRROR: MirrorName = MirrorName::from_static("directory");
pub const PERSON_NAMESPACE: &str = "exampletwin.person";

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

impl Known for KnownPersonRow {
    const NAMESPACE: &'static str = PERSON_NAMESPACE;

    fn foreign_key(&self) -> String {
        self.id.to_string()
    }

    fn upsert<'c>(&'c self, conn: &'c mut PgConnection) -> BoxFuture<'c, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO known_persons (user_id, email, display_name) \
                 VALUES ($1, $2, $3) \
                 ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email, \
                   display_name = EXCLUDED.display_name",
            )
            .bind(self.id)
            .bind(&self.email)
            .bind(&self.display_name)
            .execute(conn)
            .await?;
            Ok(())
        })
    }
}

struct ByPerson(Uuid);

impl KnownScope for ByPerson {
    const NAMESPACE: &'static str = PERSON_NAMESPACE;

    fn delete<'c>(
        &'c self,
        conn: &'c mut PgConnection,
    ) -> BoxFuture<'c, Result<Vec<String>, EngineError>> {
        Box::pin(async move {
            sqlx::query("DELETE FROM known_persons WHERE user_id = $1")
                .bind(self.0)
                .execute(conn)
                .await?;
            Ok(vec![self.0.to_string()])
        })
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
                Some(person) => {
                    cx.replace_one(KnownPersonRow {
                        id,
                        email: person.email,
                        display_name: person.display_name,
                    })
                    .await
                }
                None => cx.remove(ByPerson(id)).await,
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
                let ids: Vec<Uuid> = sqlx::query_scalar("SELECT user_id FROM known_persons")
                    .fetch_all(&pool)
                    .await?;
                Ok(ids)
            }) as BoxFuture<'static, Result<Vec<Uuid>, EngineError>>
        })
}
