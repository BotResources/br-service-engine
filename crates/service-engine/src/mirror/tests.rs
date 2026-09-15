//! Authoritative empty catalogs must reconcile; required singleton sources fail closed.
use crate::{
    Consumed,
    config::EngineConfig,
    error::EngineError,
    mirror::{Mirror, Project, Projection},
    name::{ChannelName, MirrorName, PodId},
    nats::Nats,
    transport::PgListenNotify,
};
use br_test_harness::{E2eDatabase, FabricTestNats};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
#[derive(Clone, PartialEq, Serialize, Deserialize)]
struct Entry {
    value: String,
}
impl Consumed for Entry {
    const PREFIX: &'static str = "catalog.entry.";
    const ALLOW_EMPTY: bool = true;
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
struct Required {
    value: String,
}
impl Consumed for Required {
    const PREFIX: &'static str = "required.singleton";
}
struct Catalog;
impl Project<()> for Catalog {
    type Error = EngineError;
    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        _: (),
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let values: Vec<String> = cx
                .shadow::<Entry>()
                .values()
                .map(|v| v.value.clone())
                .collect();
            sqlx::query("DELETE FROM mirrored_catalog")
                .execute(cx.conn())
                .await?;
            sqlx::query("INSERT INTO mirrored_catalog(value) SELECT unnest($1::text[])")
                .bind(values)
                .execute(cx.conn())
                .await?;
            Ok(())
        })
    }
}
struct RequiredProjection;
impl Project<()> for RequiredProjection {
    type Error = EngineError;
    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        _: (),
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let values: Vec<String> = cx
                .shadow::<Required>()
                .values()
                .map(|v| v.value.clone())
                .collect();
            sqlx::query("DELETE FROM mirrored_catalog")
                .execute(cx.conn())
                .await?;
            sqlx::query("INSERT INTO mirrored_catalog(value) SELECT unnest($1::text[])")
                .bind(values)
                .execute(cx.conn())
                .await?;
            Ok(())
        })
    }
}
#[tokio::test]
async fn empty_snapshot_last_retraction_and_restart_reconciliation_are_supported_explicitly() {
    let nats = FabricTestNats::start()
        .await
        .with_published_language()
        .await;
    let db = E2eDatabase::create(true, &[]).await;
    let pool = sqlx::PgPool::connect(&db.owner_migration_url())
        .await
        .unwrap();
    crate::schema::migrate(&pool).await.unwrap();
    sqlx::query("CREATE TABLE mirrored_catalog(value text NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    let config = EngineConfig::new(
        ChannelName::new("mirror_test").unwrap(),
        PodId::new("mirror-test").unwrap(),
    );
    let transport = Arc::new(
        PgListenNotify::connect(pool.clone(), &config)
            .await
            .unwrap(),
    );
    let bus = Nats::connect(&nats.url()).await.unwrap();
    let make = || {
        Mirror::new(MirrorName::from_static("catalog"))
            .consume::<Entry>()
            .keyed_by(|_, _| vec![()])
            .project(Catalog)
            .reconcile_keys(|_| Box::pin(async { Ok(vec![()]) }))
            .build(bus.clone(), pool.clone(), transport.clone())
    };
    let handle = make();
    handle
        .reconcile()
        .await
        .expect("an empty optional catalog bootstraps");
    let watcher = tokio::spawn(handle.watch());
    let publisher = nats.pl_publisher::<Entry>().await;
    let key = br_util_nats_fabric::KvKey::new("catalog.entry.only").unwrap();
    publisher
        .put(
            &key,
            &Entry {
                value: "present".into(),
            },
        )
        .await
        .unwrap();
    count(&pool, 1).await;
    publisher.retract(&key).await.unwrap();
    count(&pool, 0).await;
    assert!(
        !watcher.is_finished(),
        "last-key retraction must not kill the watch"
    );
    publisher
        .put(
            &key,
            &Entry {
                value: "stale while stopped".into(),
            },
        )
        .await
        .unwrap();
    count(&pool, 1).await;
    watcher.abort();
    let _ = watcher.await;
    publisher.retract(&key).await.unwrap();
    make().reconcile().await.unwrap();
    count(&pool, 0).await;
    let required = Mirror::new(MirrorName::from_static("required"))
        .consume::<Required>()
        .keyed_by(|_, _| vec![()])
        .project(RequiredProjection)
        .build(bus, pool.clone(), transport);
    assert!(
        required.reconcile().await.is_err(),
        "a missing required singleton still refuses readiness"
    );
    let required_publisher = nats.pl_publisher::<Required>().await;
    let required_key = br_util_nats_fabric::KvKey::new("required.singleton").unwrap();
    required_publisher
        .put(
            &required_key,
            &Required {
                value: "required".into(),
            },
        )
        .await
        .unwrap();
    required.reconcile().await.unwrap();
    count(&pool, 1).await;
    let required_watcher = tokio::spawn(required.watch());
    required_publisher.retract(&required_key).await.unwrap();
    let failure = tokio::time::timeout(Duration::from_secs(5), required_watcher)
        .await
        .unwrap()
        .unwrap();
    assert!(
        failure.is_err(),
        "the default policy still refuses its last key's retraction"
    );
    count(&pool, 1).await;
    pool.close().await;
    nats.shutdown().await;
    db.cleanup().await;
}
async fn count(pool: &sqlx::PgPool, expected: i64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let actual: i64 = sqlx::query_scalar("SELECT count(*) FROM mirrored_catalog")
            .fetch_one(pool)
            .await
            .unwrap();
        if actual == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected {expected} entries, got {actual}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
