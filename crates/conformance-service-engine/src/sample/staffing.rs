use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::mirror::{
    Change, Column, KnownRow, Mirror, MirrorReady, PrincipalColumn, Project, Projection, col,
};
use service_engine::name::MirrorName;
use service_engine::nats::{KvKey, Nats};
use service_engine::{Consumed, Shadows};
use sqlx::PgPool;
use uuid::Uuid;

use super::mirror::SamplePublishedUser;

pub const STAFFING_MIRROR: MirrorName = MirrorName::from_static("staffing");
const GROUP_PREFIX: &str = "identity/groups/";
const USER_PREFIX: &str = "identity/users/";
const GROUP_NAMESPACE: &str = "identity.group";
const MEMBER_NAMESPACE: &str = "identity.group_member";

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

impl KnownRow for KnownGroupRow {
    const TABLE: &'static str = "known_groups";
    const NAMESPACE: &'static str = GROUP_NAMESPACE;
    const KEY: &'static [&'static str] = &["group_id"];
    const PRINCIPAL: Option<PrincipalColumn> = None;

    fn key(&self) -> Vec<Column> {
        vec![col("group_id", self.id)]
    }

    fn values(&self) -> Vec<Column> {
        vec![col("name", self.name.clone())]
    }
}

struct KnownMemberRow {
    group_id: Uuid,
    user_id: Uuid,
}

impl KnownRow for KnownMemberRow {
    const TABLE: &'static str = "known_user_group";
    const NAMESPACE: &'static str = MEMBER_NAMESPACE;
    const KEY: &'static [&'static str] = &["group_id", "user_id"];
    const PRINCIPAL: Option<PrincipalColumn> = None;

    fn key(&self) -> Vec<Column> {
        vec![col("group_id", self.group_id), col("user_id", self.user_id)]
    }

    fn values(&self) -> Vec<Column> {
        Vec::new()
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
            let members: Vec<KnownMemberRow> = match &group {
                None => Vec::new(),
                Some(group) => {
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
                }
            };
            let Some(group) = group else {
                cx.replace_rows(vec![col("group_id", group_id)], members)
                    .await?;
                return cx
                    .retire::<KnownGroupRow>(vec![col("group_id", group_id)])
                    .await;
            };
            cx.upsert(KnownGroupRow {
                id: group_id,
                name: group.name,
            })
            .await?;
            cx.replace_rows(vec![col("group_id", group_id)], members)
                .await
                .map(|_| ())
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
