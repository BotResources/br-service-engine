use std::sync::Arc;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::RecordingTransport;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::mirror::{
    Column, KnownRow, MirrorReady, PrincipalColumn, Project, Projection, RowScope, col,
};
use service_engine::name::MirrorName;
use service_engine::nats::{KvKey, Nats};
use service_engine::{Consumed, Mirror};
use sqlx::PgPool;
use uuid::Uuid;

const PREFIX: &str = "s257project/";

#[derive(Clone, Serialize, Deserialize)]
struct PublishedProject {
    id: Uuid,
    members: Vec<Uuid>,
}

impl Consumed for PublishedProject {
    const PREFIX: &'static str = PREFIX;
}

struct MemberRow {
    project_id: Uuid,
    user_id: Uuid,
}

impl KnownRow for MemberRow {
    const TABLE: &'static str = "s257_project_member";
    const NAMESPACE: &'static str = "s257.project_member";
    const KEY: &'static [&'static str] = &["project_id", "user_id"];
    const PRINCIPAL: Option<PrincipalColumn> = None;

    fn key(&self) -> Vec<Column> {
        vec![
            col("project_id", self.project_id),
            col("user_id", self.user_id),
        ]
    }

    fn values(&self) -> Vec<Column> {
        vec![col("seen", true)]
    }
}

fn project_key(id: Uuid) -> KvKey {
    KvKey::new(format!("{PREFIX}{id}")).expect("a valid project key")
}

struct MembersProjection;

impl Project<Uuid> for MembersProjection {
    type Error = EngineError;

    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        project_ids: Vec<Uuid>,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let rows: Vec<MemberRow> = {
                let projects = cx.shadow::<PublishedProject>();
                project_ids
                    .iter()
                    .filter_map(|id| projects.get(&project_key(*id)))
                    .flat_map(|project| {
                        project.members.iter().map(|user_id| MemberRow {
                            project_id: project.id,
                            user_id: *user_id,
                        })
                    })
                    .collect()
            };
            cx.replace_rows(RowScope::any_of("project_id", project_ids), rows)
                .await
                .map(|_| ())
        })
    }
}

fn members_mirror() -> MirrorReady<Uuid, MembersProjection> {
    Mirror::new(MirrorName::from_static("s257_members"))
        .consume::<PublishedProject>()
        .keyed_by(|_, change| {
            change
                .key
                .as_str()
                .strip_prefix(PREFIX)
                .and_then(|raw| Uuid::parse_str(raw).ok())
                .into_iter()
                .collect()
        })
        .project(MembersProjection)
        .reconcile_keys(|pool: PgPool| {
            Box::pin(async move {
                Ok(
                    sqlx::query_scalar("SELECT DISTINCT project_id FROM s257_project_member")
                        .fetch_all(&pool)
                        .await?,
                )
            }) as BoxFuture<'static, Result<Vec<Uuid>, EngineError>>
        })
}

async fn provision(db: &TestDb) {
    let app = db.app_role();
    for statement in [
        "CREATE TABLE s257_project_member (project_id uuid NOT NULL, user_id uuid NOT NULL, \
         seen boolean NOT NULL, PRIMARY KEY (project_id, user_id))"
            .to_string(),
        "CREATE TABLE s257_statement (op text NOT NULL)".to_string(),
        "CREATE FUNCTION s257_count_statement() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN INSERT INTO s257_statement (op) VALUES (TG_OP); RETURN NULL; END $$"
            .to_string(),
        "CREATE TRIGGER s257_count AFTER INSERT OR UPDATE OR DELETE ON s257_project_member \
         FOR EACH STATEMENT EXECUTE FUNCTION s257_count_statement()"
            .to_string(),
        format!("GRANT SELECT, INSERT, UPDATE, DELETE ON s257_project_member TO \"{app}\""),
        format!("GRANT SELECT, INSERT, DELETE ON s257_statement TO \"{app}\""),
    ] {
        sqlx::query(&statement)
            .execute(db.owner_pool())
            .await
            .expect("provision the member table and its statement counter");
    }
}

async fn publish(nats: &Nats, id: Uuid, members: usize) {
    nats.published_language::<PublishedProject>()
        .await
        .expect("bind the published-language bucket")
        .put(
            &project_key(id),
            &PublishedProject {
                id,
                members: (0..members).map(|_| Uuid::now_v7()).collect(),
            },
        )
        .await
        .expect("publish a project the way a producer would");
}

async fn statements_spent_rescanning(nats: &Nats, pool: &PgPool) -> Vec<(String, i64)> {
    sqlx::query("DELETE FROM s257_statement")
        .execute(pool)
        .await
        .expect("reset the statement counter");
    members_mirror()
        .build(nats.clone(), pool.clone(), Arc::new(RecordingTransport))
        .reconcile()
        .await
        .expect("the mirror rescans and converges");
    sqlx::query_as("SELECT op, count(*) FROM s257_statement GROUP BY op ORDER BY op")
        .fetch_all(pool)
        .await
        .expect("read the statement counter")
}

async fn mirrored_rows(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM s257_project_member")
        .fetch_one(pool)
        .await
        .expect("count the mirrored member rows")
}

#[tokio::test]
async fn s257_a_rescan_of_many_projects_writes_the_member_table_in_the_statements_of_one() {
    let db = TestDb::fresh().await;
    provision(&db).await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    publish(&fabric, Uuid::now_v7(), 3).await;
    let first_for_one = statements_spent_rescanning(&fabric, &pool).await;
    let again_for_one = statements_spent_rescanning(&fabric, &pool).await;
    assert_eq!(mirrored_rows(&pool).await, 3);
    assert!(
        again_for_one
            .iter()
            .any(|(op, count)| op == "DELETE" && *count > 0),
        "the counter observes the rescan's writes: {again_for_one:?}"
    );

    for _ in 0..24 {
        publish(&fabric, Uuid::now_v7(), 3).await;
    }
    let first_for_many = statements_spent_rescanning(&fabric, &pool).await;
    let again_for_many = statements_spent_rescanning(&fabric, &pool).await;
    assert_eq!(
        mirrored_rows(&pool).await,
        75,
        "the rescan mirrors every member of every project"
    );

    assert_eq!(
        first_for_many, first_for_one,
        "a rescan that inserts the members of twenty-four new projects writes the member table \
         in the statements a rescan of one project spends: one statement set per table, never \
         one per key"
    );
    assert_eq!(
        again_for_many, again_for_one,
        "a rescan of twenty-five unchanged projects, the periodic reconcile's case, costs the \
         member table what a rescan of one costs"
    );

    drop(nats);
    db.cleanup().await;
}
