use std::collections::BTreeSet;
use std::sync::Arc;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::{RecordingTransport, staged_impacts};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::impact::Impact;
use service_engine::mirror::{
    Column, KnownRow, MirrorReady, PrincipalColumn, Project, Projection, RowScope, col,
};
use service_engine::name::MirrorName;
use service_engine::nats::{KvKey, Nats};
use service_engine::{Consumed, Mirror};
use sqlx::PgPool;

const PREFIX: &str = "s255tag/";
const NAMESPACE: &str = "s255.tag";

#[derive(Clone, Serialize, Deserialize)]
struct PublishedTag {
    name: String,
    label: String,
}

impl Consumed for PublishedTag {
    const PREFIX: &'static str = PREFIX;
}

struct TagRow {
    name: String,
    label: String,
}

impl KnownRow for TagRow {
    const TABLE: &'static str = "s255_tag";
    const NAMESPACE: &'static str = NAMESPACE;
    const KEY: &'static [&'static str] = &["name"];
    const PRINCIPAL: Option<PrincipalColumn> = None;

    fn key(&self) -> Vec<Column> {
        vec![col("name", self.name.clone())]
    }

    fn values(&self) -> Vec<Column> {
        vec![col("label", self.label.clone())]
    }
}

struct TagsProjection;

impl Project<()> for TagsProjection {
    type Error = EngineError;

    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        _keys: Vec<()>,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let rows: Vec<TagRow> = cx
                .shadow::<PublishedTag>()
                .values()
                .map(|tag| TagRow {
                    name: tag.name.clone(),
                    label: tag.label.clone(),
                })
                .collect();
            cx.replace_rows(RowScope::whole_table(), rows)
                .await
                .map(|_| ())
        })
    }
}

fn tags_mirror() -> MirrorReady<(), TagsProjection> {
    Mirror::new(MirrorName::from_static("s255_tags"))
        .consume::<PublishedTag>()
        .keyed_by(|_, _| vec![()])
        .project(TagsProjection)
        .reconcile_keys(|_pool: PgPool| {
            Box::pin(async { Ok(vec![()]) }) as BoxFuture<'static, Result<Vec<()>, EngineError>>
        })
}

async fn publish(nats: &Nats, name: &str, label: &str) {
    nats.published_language::<PublishedTag>()
        .await
        .expect("bind the published-language bucket")
        .put(
            &KvKey::new(format!("{PREFIX}{name}")).expect("a valid tag key"),
            &PublishedTag {
                name: name.to_string(),
                label: label.to_string(),
            },
        )
        .await
        .expect("publish a tag the way a producer would");
}

async fn reconcile(nats: &Nats, pool: &PgPool) -> BTreeSet<String> {
    sqlx::query("DELETE FROM sample_staged_impact")
        .execute(pool)
        .await
        .expect("clear the recorded impacts between runs");
    tags_mirror()
        .build(nats.clone(), pool.clone(), Arc::new(RecordingTransport))
        .reconcile()
        .await
        .expect("the mirror reconciles");
    staged_impacts(pool)
        .await
        .into_iter()
        .map(|impact| match impact {
            Impact::ForeignChanged { foreign } => {
                assert_eq!(foreign.namespace().as_str(), NAMESPACE);
                foreign.key().as_str().to_string()
            }
            other => panic!("the mirror stages only row impacts, got {other:?}"),
        })
        .collect()
}

async fn tags(pool: &PgPool) -> Vec<(String, String)> {
    sqlx::query_as("SELECT name, label FROM s255_tag ORDER BY name")
        .fetch_all(pool)
        .await
        .expect("read the mirrored tags")
}

fn names(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| name.to_string()).collect()
}

#[tokio::test]
async fn s255_a_whole_table_replace_deletes_every_row_the_set_does_not_name_and_stages_only_changes()
 {
    let db = TestDb::fresh().await;
    for statement in [
        "CREATE TABLE s255_tag (name text PRIMARY KEY, label text NOT NULL)".to_string(),
        format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON s255_tag TO \"{}\"",
            db.app_role()
        ),
        "INSERT INTO s255_tag (name, label) VALUES ('stray', 'left behind')".to_string(),
    ] {
        sqlx::query(&statement)
            .execute(db.owner_pool())
            .await
            .expect("provision the tag table with a row no producer publishes");
    }
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    publish(&fabric, "alpha", "Alpha").await;
    publish(&fabric, "beta", "Beta").await;
    assert_eq!(
        reconcile(&fabric, &pool).await,
        names(&["alpha", "beta", "stray"]),
        "the whole table is the scope: both published rows are inserted and the unnamed row is \
         deleted, each staging its key"
    );
    assert_eq!(
        tags(&pool).await,
        vec![
            ("alpha".to_string(), "Alpha".to_string()),
            ("beta".to_string(), "Beta".to_string())
        ]
    );

    assert_eq!(
        reconcile(&fabric, &pool).await,
        BTreeSet::new(),
        "a whole-table replace of identical rows stages nothing"
    );

    publish(&fabric, "beta", "Beta, renamed").await;
    assert_eq!(
        reconcile(&fabric, &pool).await,
        names(&["beta"]),
        "a changed value column stages that one row, never the rest of the table"
    );

    drop(nats);
    db.cleanup().await;
}
