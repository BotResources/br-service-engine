use std::sync::Arc;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::RecordingTransport;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::mirror::{Column, Known, KnownRow, KnownScope, Project, Projection, col};
use service_engine::name::MirrorName;
use service_engine::nats::KvKey;
use service_engine::{Consumed, Mirror};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

const ROSTER_PREFIX: &str = "roster/";
const GROUP_NAMESPACE: &str = "s209.group";
const MEMBER_NAMESPACE: &str = "s209.member";

#[derive(Clone, Serialize, Deserialize)]
struct Roster {
    id: Uuid,
    label: String,
    members: Vec<Uuid>,
}

impl Consumed for Roster {
    const PREFIX: &'static str = ROSTER_PREFIX;
}

struct GroupRow {
    id: Uuid,
    name: String,
}

impl KnownRow for GroupRow {
    const TABLE: &'static str = "known_groups";
    const NAMESPACE: &'static str = GROUP_NAMESPACE;

    fn key(&self) -> Vec<Column> {
        vec![col("group_id", self.id)]
    }

    fn values(&self) -> Vec<Column> {
        vec![col("name", self.name.clone())]
    }
}

struct MemberRow {
    group_id: Uuid,
    user_id: Uuid,
}

impl Known for MemberRow {
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

struct Members(Uuid);

impl KnownScope for Members {
    const NAMESPACE: &'static str = MEMBER_NAMESPACE;

    fn delete<'c>(
        &'c self,
        conn: &'c mut PgConnection,
    ) -> BoxFuture<'c, Result<Vec<String>, EngineError>> {
        Box::pin(async move {
            let removed: Vec<Uuid> = sqlx::query_scalar(
                "DELETE FROM known_user_group WHERE group_id = $1 RETURNING user_id",
            )
            .bind(self.0)
            .fetch_all(conn)
            .await?;
            Ok(removed.into_iter().map(|id| id.to_string()).collect())
        })
    }
}

struct RosterProjection;

impl Project<Uuid> for RosterProjection {
    type Error = EngineError;

    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        id: Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let roster = cx
                .shadow::<Roster>()
                .values()
                .find(|entry| entry.id == id)
                .cloned();
            match roster {
                Some(roster) => {
                    cx.upsert(GroupRow {
                        id,
                        name: roster.label,
                    })
                    .await?;
                    let members = roster.members.iter().map(|user_id| MemberRow {
                        group_id: id,
                        user_id: *user_id,
                    });
                    cx.replace(Members(id), members).await?;
                    Ok(())
                }
                None => {
                    cx.remove(Members(id)).await?;
                    cx.retire::<GroupRow>(vec![col("group_id", id)]).await
                }
            }
        })
    }
}

fn roster_mirror(
    name: &'static str,
) -> service_engine::mirror::MirrorReady<Uuid, RosterProjection> {
    Mirror::new(MirrorName::from_static(name))
        .consume::<Roster>()
        .keyed_by(|shadows, _| {
            shadows
                .shadow::<Roster>()
                .values()
                .map(|entry| entry.id)
                .collect()
        })
        .project(RosterProjection)
        .reconcile_keys(group_keys)
}

fn group_keys(pool: PgPool) -> BoxFuture<'static, Result<Vec<Uuid>, EngineError>> {
    Box::pin(async move {
        Ok(sqlx::query_scalar("SELECT group_id FROM known_groups")
            .fetch_all(&pool)
            .await?)
    })
}

async fn publish(nats: &service_engine::nats::Nats, key: &str, roster: &Roster) {
    nats.published_language::<Roster>()
        .await
        .expect("bind the published-language bucket")
        .put(&KvKey::new(key).expect("a valid roster key"), roster)
        .await
        .expect("publish a roster the way a producer would");
}

async fn impacts(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM sample_staged_impact")
        .fetch_one(pool)
        .await
        .expect("count the impacts the mirror staged")
}

async fn clear_impacts(pool: &PgPool) {
    sqlx::query("DELETE FROM sample_staged_impact")
        .execute(pool)
        .await
        .expect("clear the recorded impacts between runs");
}

#[tokio::test]
async fn s209_a_reconcile_that_changes_nothing_stages_no_impact() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let group = Uuid::now_v7();
    let alice = Uuid::now_v7();
    let bob = Uuid::now_v7();
    publish(
        &fabric,
        "roster/one",
        &Roster {
            id: group,
            label: "engineering".to_string(),
            members: vec![alice, bob],
        },
    )
    .await;

    roster_mirror("roster_first")
        .build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport))
        .reconcile()
        .await
        .expect("the first boot projects the roster");
    assert_eq!(
        impacts(&pool).await,
        3,
        "the group upsert and both member inserts each stage one impact"
    );

    clear_impacts(&pool).await;
    roster_mirror("roster_first")
        .build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport))
        .reconcile()
        .await
        .expect("a second reconcile of the identical roster converges");
    assert_eq!(
        impacts(&pool).await,
        0,
        "an unchanged group row and an unchanged member set stage nothing"
    );

    clear_impacts(&pool).await;
    publish(
        &fabric,
        "roster/one",
        &Roster {
            id: group,
            label: "platform".to_string(),
            members: vec![alice, bob],
        },
    )
    .await;
    roster_mirror("roster_first")
        .build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport))
        .reconcile()
        .await
        .expect("the label change reconciles");
    assert_eq!(
        impacts(&pool).await,
        1,
        "one changed value column stages exactly one impact"
    );

    clear_impacts(&pool).await;
    publish(
        &fabric,
        "roster/one",
        &Roster {
            id: group,
            label: "platform".to_string(),
            members: vec![alice],
        },
    )
    .await;
    roster_mirror("roster_first")
        .build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport))
        .reconcile()
        .await
        .expect("the member removal reconciles");
    assert_eq!(
        impacts(&pool).await,
        1,
        "a replace that drops one key stages exactly one remove impact"
    );

    drop(nats);
    db.cleanup().await;
}
