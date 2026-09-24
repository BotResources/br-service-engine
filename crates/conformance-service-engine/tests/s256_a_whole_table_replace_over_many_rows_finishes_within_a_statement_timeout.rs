use std::str::FromStr;
use std::sync::Arc;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::{RecordingTransport, staged_impacts};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::mirror::{
    Column, KnownRow, MirrorReady, PrincipalColumn, Project, Projection, RowScope, col,
};
use service_engine::name::MirrorName;
use service_engine::{Consumed, Mirror};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use uuid::Uuid;

const KEPT: u128 = 100_000;
const STRAYS: u128 = 3;
const STATEMENT_TIMEOUT: &str = "10s";

#[derive(Clone, Serialize, Deserialize)]
struct PublishedPair;

impl Consumed for PublishedPair {
    const PREFIX: &'static str = "s256pair/";
}

struct PairRow(u128);

impl KnownRow for PairRow {
    const TABLE: &'static str = "s256_pair";
    const NAMESPACE: &'static str = "s256.pair";
    const KEY: &'static [&'static str] = &["left_id", "right_id"];
    const PRINCIPAL: Option<PrincipalColumn> = None;

    fn key(&self) -> Vec<Column> {
        vec![
            col("left_id", Uuid::from_u128(self.0 / 100)),
            col("right_id", Uuid::from_u128(self.0)),
        ]
    }

    fn values(&self) -> Vec<Column> {
        vec![col("weight", 1_i64)]
    }
}

struct KeptPairs;

impl Project<()> for KeptPairs {
    type Error = EngineError;

    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        _keys: Vec<()>,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let rows: Vec<PairRow> = (0..KEPT).map(PairRow).collect();
            cx.replace_rows(RowScope::whole_table(), rows)
                .await
                .map(|_| ())
        })
    }
}

fn pairs_mirror() -> MirrorReady<(), KeptPairs> {
    Mirror::new(MirrorName::from_static("s256_pairs"))
        .consume::<PublishedPair>()
        .keyed_by(|_, _| vec![()])
        .project(KeptPairs)
        .reconcile_keys(|_pool: PgPool| {
            Box::pin(async { Ok(vec![()]) }) as BoxFuture<'static, Result<Vec<()>, EngineError>>
        })
}

async fn bounded_app_pool(db: &TestDb) -> PgPool {
    let options = PgConnectOptions::from_str(&db.url_as(db.app_role()))
        .expect("the app role url parses")
        .options([("statement_timeout", STATEMENT_TIMEOUT)]);
    PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .expect("an app-role pool whose every statement is bounded in time")
}

async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(pool)
        .await
        .expect("count the mirrored pairs")
}

#[tokio::test]
async fn s256_a_whole_table_replace_over_100k_two_uuid_keys_retires_the_strays_within_a_statement_timeout()
 {
    let db = TestDb::fresh().await;
    for statement in [
        "CREATE TABLE s256_pair (left_id uuid NOT NULL, right_id uuid NOT NULL, weight bigint \
         NOT NULL, PRIMARY KEY (left_id, right_id))"
            .to_string(),
        format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON s256_pair TO \"{}\"",
            db.app_role()
        ),
        format!(
            "INSERT INTO s256_pair (left_id, right_id, weight) SELECT lpad(to_hex(i / 100), 32, \
             '0')::uuid, lpad(to_hex(i), 32, '0')::uuid, 1 FROM generate_series(0, {}) AS i",
            KEPT + STRAYS - 1
        ),
        "ANALYZE s256_pair".to_string(),
    ] {
        sqlx::query(&statement)
            .execute(db.owner_pool())
            .await
            .expect("provision the pair table with every kept row and a few strays");
    }
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = bounded_app_pool(&db).await;

    pairs_mirror()
        .build(fabric, pool.clone(), Arc::new(RecordingTransport))
        .reconcile()
        .await
        .expect("the mirror reconciles");

    assert_eq!(
        count(&pool, "SELECT count(*) FROM s256_pair").await,
        i64::try_from(KEPT).expect("the kept count fits"),
        "every stray row is retired and every kept row stays, each statement within {}",
        STATEMENT_TIMEOUT
    );
    assert_eq!(
        staged_impacts(&pool).await.len(),
        usize::try_from(STRAYS).expect("the stray count fits"),
        "a replace of 100k unchanged rows stages only the retired strays"
    );

    drop(nats);
    db.cleanup().await;
}
