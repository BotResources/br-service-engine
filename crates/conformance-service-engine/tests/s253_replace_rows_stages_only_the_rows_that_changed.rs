use std::collections::BTreeSet;
use std::sync::Arc;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::{RecordingTransport, staged_impacts};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::impact::{Deps, Impact};
use service_engine::mirror::{
    Column, KnownRow, MirrorReady, PrincipalColumn, Project, Projection, RowScope, col,
};
use service_engine::name::MirrorName;
use service_engine::nats::{KvKey, Nats};
use service_engine::{Consumed, Mirror};
use sqlx::PgPool;
use uuid::Uuid;

const PREFIX: &str = "s253project/";
const NAMESPACE: &str = "s253.project_member";
const DEP_PROJECT_ADMIN: Deps = Deps::from_bits(1 << 3);

#[derive(Clone, Serialize, Deserialize)]
struct PublishedProject {
    id: Uuid,
    members: Vec<(Uuid, bool)>,
}

impl Consumed for PublishedProject {
    const PREFIX: &'static str = PREFIX;
}

struct MemberRow {
    project_id: Uuid,
    user_id: Uuid,
    is_admin: bool,
}

impl KnownRow for MemberRow {
    const TABLE: &'static str = "s253_project_member";
    const NAMESPACE: &'static str = NAMESPACE;
    const KEY: &'static [&'static str] = &["project_id", "user_id"];
    const PRINCIPAL: Option<PrincipalColumn> = Some(PrincipalColumn {
        column: "user_id",
        deps: DEP_PROJECT_ADMIN,
    });

    fn key(&self) -> Vec<Column> {
        vec![
            col("project_id", self.project_id),
            col("user_id", self.user_id),
        ]
    }

    fn values(&self) -> Vec<Column> {
        vec![col("is_admin", self.is_admin)]
    }
}

struct MembersProjection;

impl Project<Uuid> for MembersProjection {
    type Error = EngineError;

    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        project_id: Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let rows: Vec<MemberRow> = cx
                .shadow::<PublishedProject>()
                .values()
                .filter(|project| project.id == project_id)
                .flat_map(|project| project.members.clone())
                .map(|(user_id, is_admin)| MemberRow {
                    project_id,
                    user_id,
                    is_admin,
                })
                .collect();
            cx.replace_rows(RowScope::by(col("project_id", project_id)), rows)
                .await
                .map(|_| ())
        })
    }
}

fn members_mirror() -> MirrorReady<Uuid, MembersProjection> {
    Mirror::new(MirrorName::from_static("s253_members"))
        .consume::<PublishedProject>()
        .keyed_by(|shadows, _| {
            shadows
                .shadow::<PublishedProject>()
                .values()
                .map(|project| project.id)
                .collect()
        })
        .project(MembersProjection)
        .reconcile_keys(|pool: PgPool| {
            Box::pin(async move {
                Ok(
                    sqlx::query_scalar("SELECT DISTINCT project_id FROM s253_project_member")
                        .fetch_all(&pool)
                        .await?,
                )
            }) as BoxFuture<'static, Result<Vec<Uuid>, EngineError>>
        })
}

async fn publish(nats: &Nats, project: &PublishedProject) {
    nats.published_language::<PublishedProject>()
        .await
        .expect("bind the published-language bucket")
        .put(
            &KvKey::new(format!("{PREFIX}{}", project.id)).expect("a valid project key"),
            project,
        )
        .await
        .expect("publish a project the way a producer would");
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Staged {
    rows: BTreeSet<String>,
    principals: BTreeSet<Uuid>,
}

async fn reconcile(nats: &Nats, pool: &PgPool) -> Staged {
    sqlx::query("DELETE FROM sample_staged_impact")
        .execute(pool)
        .await
        .expect("clear the recorded impacts between runs");
    members_mirror()
        .build(nats.clone(), pool.clone(), Arc::new(RecordingTransport))
        .reconcile()
        .await
        .expect("the mirror reconciles");
    let mut staged = Staged::default();
    for impact in staged_impacts(pool).await {
        match impact {
            Impact::ForeignChanged { foreign } => {
                assert_eq!(foreign.namespace().as_str(), NAMESPACE);
                staged.rows.insert(foreign.key().as_str().to_string());
            }
            Impact::PrincipalFactsChanged { principal, deps } => {
                assert_eq!(
                    deps, DEP_PROJECT_ADMIN,
                    "the row's declared deps ride along"
                );
                staged.principals.insert(principal.as_uuid());
            }
            other => panic!("the mirror stages only row and principal impacts, got {other:?}"),
        }
    }
    staged
}

async fn members(pool: &PgPool, project: Uuid) -> Vec<(Uuid, bool)> {
    sqlx::query_as(
        "SELECT user_id, is_admin FROM s253_project_member WHERE project_id = $1 ORDER BY user_id",
    )
    .bind(project)
    .fetch_all(pool)
    .await
    .expect("read the mirrored members")
}

fn staged(project: Uuid, users: &[Uuid]) -> Staged {
    Staged {
        rows: users
            .iter()
            .map(|user| format!("{project}/{user}"))
            .collect(),
        principals: users.iter().copied().collect(),
    }
}

#[tokio::test]
async fn s253_a_non_key_column_change_stages_exactly_that_row_and_its_principal_and_nothing_else() {
    let db = TestDb::fresh().await;
    for statement in [
        "CREATE TABLE s253_project_member (project_id uuid NOT NULL, user_id uuid NOT NULL, \
         is_admin boolean NOT NULL, PRIMARY KEY (project_id, user_id))"
            .to_string(),
        format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON s253_project_member TO \"{}\"",
            db.app_role()
        ),
    ] {
        sqlx::query(&statement)
            .execute(db.owner_pool())
            .await
            .expect("provision the member table the mirror writes");
    }
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let (project, other) = (Uuid::now_v7(), Uuid::now_v7());
    let (alice, bob, carol) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    let staffed = |alice_admin: bool, with_alice: bool| PublishedProject {
        id: project,
        members: [(alice, alice_admin), (bob, false)]
            .into_iter()
            .filter(|(user, _)| with_alice || *user != alice)
            .collect(),
    };
    publish(&fabric, &staffed(false, true)).await;
    publish(
        &fabric,
        &PublishedProject {
            id: other,
            members: vec![(carol, true)],
        },
    )
    .await;

    let mut first = staged(project, &[alice, bob]);
    let carol_staged = staged(other, &[carol]);
    first.rows.extend(carol_staged.rows);
    first.principals.extend(carol_staged.principals);
    assert_eq!(
        reconcile(&fabric, &pool).await,
        first,
        "every inserted row stages its own key and the principal it names"
    );

    assert_eq!(
        reconcile(&fabric, &pool).await,
        Staged::default(),
        "a reconcile of identical rows stages nothing"
    );

    publish(&fabric, &staffed(true, true)).await;
    assert_eq!(
        reconcile(&fabric, &pool).await,
        staged(project, &[alice]),
        "a change to is_admin, a non-key column, stages that one row and refreshes only alice's \
         principal facts, never bob's"
    );
    let mut expected = vec![(alice, true), (bob, false)];
    expected.sort();
    assert_eq!(members(&pool, project).await, expected);

    publish(&fabric, &staffed(true, false)).await;
    assert_eq!(
        reconcile(&fabric, &pool).await,
        staged(project, &[alice]),
        "a row that leaves the set is deleted and stages its key and its principal"
    );
    assert_eq!(members(&pool, project).await, vec![(bob, false)]);
    assert_eq!(
        members(&pool, other).await,
        vec![(carol, true)],
        "a replace scoped to one project never deletes another project's rows"
    );

    drop(nats);
    db.cleanup().await;
}
