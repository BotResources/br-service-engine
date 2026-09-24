use std::sync::Arc;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::RecordingTransport;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::mirror::{Column, KnownRow, PrincipalColumn, Project, Projection, col};
use service_engine::name::MirrorName;
use service_engine::nats::KvKey;
use service_engine::{Consumed, Mirror};
use sqlx::PgPool;
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
    const KEY: &'static [&'static str] = &["group_id"];
    const PRINCIPAL: Option<PrincipalColumn> = None;

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

impl KnownRow for MemberRow {
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
                    cx.replace_rows(vec![col("group_id", id)], members).await?;
                    Ok(())
                }
                None => {
                    cx.replace_rows(vec![col("group_id", id)], Vec::<MemberRow>::new())
                        .await?;
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
