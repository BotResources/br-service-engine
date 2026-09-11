use std::collections::BTreeMap;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::mirror::{
    Change, Known, KnownScope, Mirror, MirrorHandle, MirrorReady, MirrorRun, Project, Projection,
};
use service_engine::name::MirrorName;
use service_engine::nats::{KvKey, Nats};
use service_engine::transport::ImpactTransport;
use service_engine::{Consumed, Shadows};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

pub const DIRECTORY_MIRROR: MirrorName = MirrorName::from_static("directory");
const USER_PREFIX: &str = "identity/users/";
const USER_NAMESPACE: &str = "identity.user";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SamplePublishedUser {
    pub email: String,
}

impl Consumed for SamplePublishedUser {
    const PREFIX: &'static str = USER_PREFIX;
}

pub struct SampleDirectory {
    users: BTreeMap<Uuid, SamplePublishedUser>,
}

impl SampleDirectory {
    pub fn with_users(users: &[(Uuid, &str)]) -> Self {
        Self {
            users: users
                .iter()
                .map(|(id, email)| {
                    (
                        *id,
                        SamplePublishedUser {
                            email: (*email).to_string(),
                        },
                    )
                })
                .collect(),
        }
    }
}

fn user_key(id: Uuid) -> KvKey {
    KvKey::new(format!("{USER_PREFIX}{id}")).expect("a published-user key is valid")
}

fn id_of(key: &KvKey) -> Option<Uuid> {
    key.as_str()
        .strip_prefix(USER_PREFIX)
        .and_then(|raw| Uuid::parse_str(raw).ok())
}

pub async fn publish_roster(nats: &Nats, roster: &SampleDirectory) {
    let bucket = nats
        .published_language::<SamplePublishedUser>()
        .await
        .expect("bind the published-language bucket");
    for (id, user) in &roster.users {
        bucket
            .put(&user_key(*id), user)
            .await
            .expect("publish a roster row the way identity would");
    }
}

pub async fn retract_user(nats: &Nats, id: Uuid) {
    let bucket = nats
        .published_language::<SamplePublishedUser>()
        .await
        .expect("bind the published-language bucket");
    bucket
        .retract(&user_key(id))
        .await
        .expect("retract a roster row the way identity would");
}

struct KnownUser {
    id: Uuid,
    email: String,
}

impl Known for KnownUser {
    const NAMESPACE: &'static str = USER_NAMESPACE;

    fn foreign_key(&self) -> String {
        self.id.to_string()
    }

    fn upsert<'c>(&'c self, conn: &'c mut PgConnection) -> BoxFuture<'c, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO known_users (user_id, email) VALUES ($1, $2) \
                 ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email",
            )
            .bind(self.id)
            .bind(&self.email)
            .execute(conn)
            .await?;
            Ok(())
        })
    }
}

struct ByUser(Uuid);

impl KnownScope for ByUser {
    const NAMESPACE: &'static str = USER_NAMESPACE;

    fn delete<'c>(
        &'c self,
        conn: &'c mut PgConnection,
    ) -> BoxFuture<'c, Result<Vec<String>, EngineError>> {
        Box::pin(async move {
            sqlx::query("DELETE FROM known_users WHERE user_id = $1")
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
            let user = cx
                .shadow::<SamplePublishedUser>()
                .get(&user_key(id))
                .cloned();
            match user {
                Some(user) => {
                    cx.replace_one(KnownUser {
                        id,
                        email: user.email,
                    })
                    .await
                }
                None => cx.remove(ByUser(id)).await,
            }
        })
    }
}

pub fn directory_mirror() -> MirrorReady<Uuid, impl Project<Uuid>> {
    Mirror::new(DIRECTORY_MIRROR)
        .consume::<SamplePublishedUser>()
        .keyed_by(|_shadows: &Shadows, change: &Change| {
            id_of(&change.key).into_iter().collect::<Vec<Uuid>>()
        })
        .project(DirectoryProjection)
        .reconcile_keys(|pool: PgPool| {
            Box::pin(async move {
                let ids: Vec<Uuid> = sqlx::query_scalar("SELECT user_id FROM known_users")
                    .fetch_all(&pool)
                    .await?;
                Ok(ids)
            }) as BoxFuture<'static, Result<Vec<Uuid>, EngineError>>
        })
}

pub fn directory_mirror_handle(
    nats: Nats,
    pool: PgPool,
    transport: Arc<dyn ImpactTransport>,
) -> MirrorHandle {
    let backfill_pool = pool.clone();
    directory_mirror()
        .build(nats, pool, transport)
        .with_backfill(move || {
            let pool = backfill_pool.clone();
            Box::pin(async move { backfill(&pool).await }) as MirrorRun
        })
}

async fn backfill(pool: &PgPool) -> Result<(), EngineError> {
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM known_users")
        .fetch_one(pool)
        .await?;
    sqlx::query("INSERT INTO sample_backfill (id, mirror, rows_seen) VALUES ($1, $2, $3)")
        .bind(Uuid::now_v7())
        .bind(DIRECTORY_MIRROR.as_str())
        .bind(rows)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn backfills(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM sample_backfill")
        .fetch_one(pool)
        .await
        .expect("count the one-shot backfills the mirror ran")
}

pub async fn known_users(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM known_users")
        .fetch_one(pool)
        .await
        .expect("count the mirrored roster rows")
}
