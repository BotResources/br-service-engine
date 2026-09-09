use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::mirror::{Change, Known, KnownScope, Mirror, MirrorReady, Project, Projection};
use service_engine::name::MirrorName;
use service_engine::nats::{KvKey, Nats};
use service_engine::{Consumed, Shadows};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use super::mirror::SamplePublishedUser;

pub const STAFFING_MIRROR: MirrorName = MirrorName::from_static("staffing");
const GROUP_PREFIX: &str = "identity/groups/";
const USER_PREFIX: &str = "identity/users/";
const GROUP_NAMESPACE: &str = "identity.group";
const MEMBER_NAMESPACE: &str = "identity.user";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SamplePublishedGroup {
    pub name: String,
    pub members: Vec<Uuid>,
}

impl Consumed for SamplePublishedGroup {
    const PREFIX: &'static str = GROUP_PREFIX;
}

fn group_key(id: Uuid) -> KvKey {
    KvKey::new(format!("{GROUP_PREFIX}{id}")).expect("a published-group key is valid")
}

fn user_key(id: Uuid) -> KvKey {
    KvKey::new(format!("{USER_PREFIX}{id}")).expect("a published-user key is valid")
}

fn group_id_of(key: &KvKey) -> Option<Uuid> {
    key.as_str()
        .strip_prefix(GROUP_PREFIX)
        .and_then(|raw| Uuid::parse_str(raw).ok())
}

fn user_id_of(key: &KvKey) -> Option<Uuid> {
    key.as_str()
        .strip_prefix(USER_PREFIX)
        .and_then(|raw| Uuid::parse_str(raw).ok())
}

pub async fn publish_group(nats: &Nats, id: Uuid, name: &str, members: &[Uuid]) {
    let bucket = nats
        .published_language::<SamplePublishedGroup>()
        .await
        .expect("bind the published-language bucket");
    bucket
        .put(
            &group_key(id),
            &SamplePublishedGroup {
                name: name.to_string(),
                members: members.to_vec(),
            },
        )
        .await
        .expect("publish a group the way identity would");
}

pub async fn retract_user_membership(nats: &Nats, id: Uuid) {
    let bucket = nats
        .published_language::<SamplePublishedUser>()
        .await
        .expect("bind the published-language bucket");
    bucket
        .retract(&user_key(id))
        .await
        .expect("retract a user the way identity would");
}

struct KnownGroupRow {
    id: Uuid,
    name: String,
}

impl Known for KnownGroupRow {
    const NAMESPACE: &'static str = GROUP_NAMESPACE;

    fn foreign_key(&self) -> String {
        self.id.to_string()
    }

    fn upsert<'c>(&'c self, conn: &'c mut PgConnection) -> BoxFuture<'c, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO known_groups (group_id, name) VALUES ($1, $2) \
                 ON CONFLICT (group_id) DO UPDATE SET name = EXCLUDED.name",
            )
            .bind(self.id)
            .bind(&self.name)
            .execute(conn)
            .await?;
            Ok(())
        })
    }
}

struct KnownMemberRow {
    group_id: Uuid,
    user_id: Uuid,
}

impl Known for KnownMemberRow {
    const NAMESPACE: &'static str = MEMBER_NAMESPACE;

    fn foreign_key(&self) -> String {
        self.user_id.to_string()
    }

    fn upsert<'c>(&'c self, conn: &'c mut PgConnection) -> BoxFuture<'c, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO known_user_group (group_id, user_id) VALUES ($1, $2) \
                 ON CONFLICT (group_id, user_id) DO NOTHING",
            )
            .bind(self.group_id)
            .bind(self.user_id)
            .execute(conn)
            .await?;
            Ok(())
        })
    }
}

struct ByGroup(Uuid);

impl KnownScope for ByGroup {
    const NAMESPACE: &'static str = GROUP_NAMESPACE;

    fn delete<'c>(
        &'c self,
        conn: &'c mut PgConnection,
    ) -> BoxFuture<'c, Result<Vec<String>, EngineError>> {
        Box::pin(async move {
            sqlx::query("DELETE FROM known_groups WHERE group_id = $1")
                .bind(self.0)
                .execute(conn)
                .await?;
            Ok(vec![self.0.to_string()])
        })
    }
}

struct GroupMembers(Uuid);

impl KnownScope for GroupMembers {
    const NAMESPACE: &'static str = MEMBER_NAMESPACE;

    fn delete<'c>(
        &'c self,
        conn: &'c mut PgConnection,
    ) -> BoxFuture<'c, Result<Vec<String>, EngineError>> {
        Box::pin(async move {
            let rows: Vec<Uuid> = sqlx::query_scalar(
                "DELETE FROM known_user_group WHERE group_id = $1 RETURNING user_id",
            )
            .bind(self.0)
            .fetch_all(conn)
            .await?;
            Ok(rows.into_iter().map(|id| id.to_string()).collect())
        })
    }
}

struct StaffingProjection;

impl Project<Uuid> for StaffingProjection {
    type Error = EngineError;

    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        group_id: Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let group = cx
                .shadow::<SamplePublishedGroup>()
                .get(&group_key(group_id))
                .cloned();
            let Some(group) = group else {
                return cx.remove(ByGroup(group_id)).await;
            };
            cx.replace_one(KnownGroupRow {
                id: group_id,
                name: group.name,
            })
            .await?;
            let members: Vec<KnownMemberRow> = {
                let users = cx.shadow::<SamplePublishedUser>();
                group
                    .members
                    .iter()
                    .filter(|id| users.get(&user_key(**id)).is_some())
                    .map(|id| KnownMemberRow {
                        group_id,
                        user_id: *id,
                    })
                    .collect()
            };
            cx.replace(GroupMembers(group_id), members).await
        })
    }
}

pub fn staffing_mirror() -> MirrorReady<Uuid, impl Project<Uuid>> {
    Mirror::new(STAFFING_MIRROR)
        .consume::<SamplePublishedUser>()
        .consume::<SamplePublishedGroup>()
        .keyed_by(|shadows: &Shadows, change: &Change| {
            if change.prefix == GROUP_PREFIX {
                return group_id_of(&change.key).into_iter().collect();
            }
            let Some(user) = user_id_of(&change.key) else {
                return Vec::new();
            };
            shadows
                .shadow::<SamplePublishedGroup>()
                .iter()
                .filter(|(_, group)| group.members.contains(&user))
                .filter_map(|(key, _)| group_id_of(key))
                .collect()
        })
        .project(StaffingProjection)
        .reconcile_keys(|pool: PgPool| {
            Box::pin(async move {
                let ids: Vec<Uuid> = sqlx::query_scalar("SELECT group_id FROM known_groups")
                    .fetch_all(&pool)
                    .await?;
                Ok(ids)
            }) as BoxFuture<'static, Result<Vec<Uuid>, EngineError>>
        })
}

pub async fn known_members(pool: &PgPool, group: Uuid) -> Vec<Uuid> {
    sqlx::query_scalar("SELECT user_id FROM known_user_group WHERE group_id = $1 ORDER BY user_id")
        .bind(group)
        .fetch_all(pool)
        .await
        .expect("read the mirrored group membership")
}

pub async fn known_group_name(pool: &PgPool, group: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT name FROM known_groups WHERE group_id = $1")
        .bind(group)
        .fetch_optional(pool)
        .await
        .expect("read the mirrored group")
}
