use futures_util::future::BoxFuture;
use service_engine::error::EngineError;
use service_engine::mirror::{
    Change, Column, KnownRow, Mirror, MirrorReady, Project, Projection, col,
};
use service_engine::name::MirrorName;
use service_engine::nats::KvKey;
use service_engine::{Consumed, Shadows};
use sqlx::PgPool;
use uuid::Uuid;

use super::mirror::SamplePublishedUser;
use super::staffing::SamplePublishedGroup;

pub const DECLARATIVE_MIRROR: MirrorName = MirrorName::from_static("declarative");
const USER_NAMESPACE: &str = "identity.user";
const GROUP_NAMESPACE: &str = "identity.group";

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum DeclKey {
    User(Uuid),
    Group(Uuid),
}

struct KnownUserRow {
    id: Uuid,
    email: String,
}

impl KnownRow for KnownUserRow {
    const TABLE: &'static str = "known_users";
    const NAMESPACE: &'static str = USER_NAMESPACE;

    fn key(&self) -> Vec<Column> {
        vec![col("user_id", self.id)]
    }

    fn values(&self) -> Vec<Column> {
        vec![col("email", self.email.clone())]
    }
}

struct KnownGroupRow {
    id: Uuid,
    name: String,
}

impl KnownRow for KnownGroupRow {
    const TABLE: &'static str = "known_groups";
    const NAMESPACE: &'static str = GROUP_NAMESPACE;

    fn key(&self) -> Vec<Column> {
        vec![col("group_id", self.id)]
    }

    fn values(&self) -> Vec<Column> {
        vec![col("name", self.name.clone())]
    }
}

fn user_key(id: Uuid) -> KvKey {
    KvKey::new(format!("{}{id}", SamplePublishedUser::PREFIX)).expect("a user key is valid")
}

fn group_key(id: Uuid) -> KvKey {
    KvKey::new(format!("{}{id}", SamplePublishedGroup::PREFIX)).expect("a group key is valid")
}

fn id_under(prefix: &str, key: &KvKey) -> Option<Uuid> {
    key.as_str()
        .strip_prefix(prefix)
        .and_then(|raw| Uuid::parse_str(raw).ok())
}

struct DeclarativeProjection;

impl Project<DeclKey> for DeclarativeProjection {
    type Error = EngineError;

    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        key: DeclKey,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            match key {
                DeclKey::User(id) => {
                    match cx
                        .shadow::<SamplePublishedUser>()
                        .get(&user_key(id))
                        .cloned()
                    {
                        Some(user) => cx
                            .upsert(KnownUserRow {
                                id,
                                email: user.email,
                            })
                            .await
                            .map(|_| ()),
                        None => cx.retire::<KnownUserRow>(vec![col("user_id", id)]).await,
                    }
                }
                DeclKey::Group(id) => {
                    match cx
                        .shadow::<SamplePublishedGroup>()
                        .get(&group_key(id))
                        .cloned()
                    {
                        Some(group) => cx
                            .upsert(KnownGroupRow {
                                id,
                                name: group.name,
                            })
                            .await
                            .map(|_| ()),
                        None => cx.retire::<KnownGroupRow>(vec![col("group_id", id)]).await,
                    }
                }
            }
        })
    }
}

pub fn declarative_mirror() -> MirrorReady<DeclKey, impl Project<DeclKey>> {
    Mirror::new(DECLARATIVE_MIRROR)
        .consume::<SamplePublishedUser>()
        .consume::<SamplePublishedGroup>()
        .keyed_by(|_shadows: &Shadows, change: &Change| {
            if change.prefix == SamplePublishedGroup::PREFIX {
                return id_under(SamplePublishedGroup::PREFIX, &change.key)
                    .map(DeclKey::Group)
                    .into_iter()
                    .collect();
            }
            id_under(SamplePublishedUser::PREFIX, &change.key)
                .map(DeclKey::User)
                .into_iter()
                .collect()
        })
        .project(DeclarativeProjection)
        .reconcile_keys(|pool: PgPool| {
            Box::pin(async move {
                let mut keys = Vec::new();
                let users: Vec<Uuid> = sqlx::query_scalar("SELECT user_id FROM known_users")
                    .fetch_all(&pool)
                    .await?;
                keys.extend(users.into_iter().map(DeclKey::User));
                let groups: Vec<Uuid> = sqlx::query_scalar("SELECT group_id FROM known_groups")
                    .fetch_all(&pool)
                    .await?;
                keys.extend(groups.into_iter().map(DeclKey::Group));
                Ok(keys)
            }) as BoxFuture<'static, Result<Vec<DeclKey>, EngineError>>
        })
}

pub async fn known_user_email(pool: &PgPool, id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT email FROM known_users WHERE user_id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .expect("read one declaratively-mirrored user")
}

pub async fn known_group_name(pool: &PgPool, id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT name FROM known_groups WHERE group_id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .expect("read one declaratively-mirrored group")
}
