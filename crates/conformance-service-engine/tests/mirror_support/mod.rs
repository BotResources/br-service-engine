#![allow(dead_code)]
//! A consumed catalog whose projection key lives only in the payload, so a
//! retract cannot derive it from the KV key. The engine's own sample directory
//! mirror keys off the KV key and cannot exercise that path.

use std::time::Duration;

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::mirror::{MirrorReady, Project, Projection};
use service_engine::name::MirrorName;
use service_engine::nats::{KvKey, Nats};
use service_engine::{Consumed, Mirror};
use sqlx::PgPool;
use uuid::Uuid;

pub const CATALOG_PREFIX: &str = "catalog/";
pub const PUBLISHED_LANGUAGE: &str = "PUBLISHED_LANGUAGE";
pub const OBSERVED_WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(20);

#[derive(Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub id: Uuid,
    pub value: String,
}

impl Consumed for CatalogEntry {
    const PREFIX: &'static str = CATALOG_PREFIX;
}

/// The same shape in a second bucket, to prove identities and boundaries are
/// per bucket even when two consumptions share a prefix.
#[derive(Clone, Serialize, Deserialize)]
pub struct OtherEntry {
    pub id: Uuid,
    pub value: String,
}

impl Consumed for OtherEntry {
    const PREFIX: &'static str = CATALOG_PREFIX;

    fn bucket() -> &'static str {
        OTHER_LANGUAGE
    }
}

pub const OTHER_LANGUAGE: &str = "OTHER_LANGUAGE";

pub struct CatalogProjection;

impl Project<Uuid> for CatalogProjection {
    type Error = EngineError;

    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        id: Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let value = value_of(&cx, id);
            match value {
                Some(value) => {
                    sqlx::query(
                        "INSERT INTO known_users (user_id, email) VALUES ($1, $2) \
                         ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email",
                    )
                    .bind(id)
                    .bind(value)
                    .execute(cx.conn())
                    .await?;
                }
                None => {
                    sqlx::query("DELETE FROM known_users WHERE user_id = $1")
                        .bind(id)
                        .execute(cx.conn())
                        .await?;
                }
            }
            Ok(())
        })
    }
}

fn value_of(cx: &Projection<'_>, id: Uuid) -> Option<String> {
    cx.shadow::<CatalogEntry>()
        .values()
        .find(|entry| entry.id == id)
        .map(|entry| entry.value.clone())
        .or_else(|| {
            cx.shadow::<OtherEntry>()
                .values()
                .find(|entry| entry.id == id)
                .map(|entry| entry.value.clone())
        })
}

/// A projector that writes, then fails, to prove a failed reconcile commits
/// neither the projection nor the watermark.
pub struct FailingProjection;

impl Project<Uuid> for FailingProjection {
    type Error = EngineError;

    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        id: Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            sqlx::query("DELETE FROM known_users WHERE user_id = $1")
                .bind(id)
                .execute(cx.conn())
                .await?;
            Err(EngineError::Config("deliberate projector failure".into()))
        })
    }
}

pub fn catalog(name: &'static str) -> MirrorReady<Uuid, CatalogProjection> {
    Mirror::new(MirrorName::from_static(name))
        .consume::<CatalogEntry>()
        .keyed_by(|shadows, _| {
            shadows
                .shadow::<CatalogEntry>()
                .values()
                .map(|entry| entry.id)
                .collect()
        })
        .project(CatalogProjection)
        .reconcile_keys(persisted_keys)
}

/// The same catalog joined with a second bucket that carries the same prefix.
pub fn joined(name: &'static str) -> MirrorReady<Uuid, CatalogProjection> {
    Mirror::new(MirrorName::from_static(name))
        .consume::<CatalogEntry>()
        .consume::<OtherEntry>()
        .keyed_by(|shadows, _| {
            shadows
                .shadow::<CatalogEntry>()
                .values()
                .map(|entry| entry.id)
                .chain(
                    shadows
                        .shadow::<OtherEntry>()
                        .values()
                        .map(|entry| entry.id),
                )
                .collect()
        })
        .project(CatalogProjection)
        .reconcile_keys(persisted_keys)
}

pub fn failing(name: &'static str) -> MirrorReady<Uuid, FailingProjection> {
    Mirror::new(MirrorName::from_static(name))
        .consume::<CatalogEntry>()
        .keyed_by(|shadows, _| {
            shadows
                .shadow::<CatalogEntry>()
                .values()
                .map(|entry| entry.id)
                .collect()
        })
        .project(FailingProjection)
        .reconcile_keys(persisted_keys)
}

pub fn persisted_keys(pool: PgPool) -> BoxFuture<'static, Result<Vec<Uuid>, EngineError>> {
    Box::pin(async move {
        Ok(sqlx::query_scalar("SELECT user_id FROM known_users")
            .fetch_all(&pool)
            .await?)
    })
}

pub async fn publish(fabric: &Nats, key: &str, id: Uuid, value: &str) {
    fabric
        .published_language::<CatalogEntry>()
        .await
        .expect("bind the published-language bucket")
        .put(
            &KvKey::new(key).expect("a valid catalog key"),
            &CatalogEntry {
                id,
                value: value.to_string(),
            },
        )
        .await
        .expect("publish a catalog row the way a producer would");
}

pub async fn retract(fabric: &Nats, key: &str) {
    fabric
        .published_language::<CatalogEntry>()
        .await
        .expect("bind the published-language bucket")
        .retract(&KvKey::new(key).expect("a valid catalog key"))
        .await
        .expect("retract a catalog row the way a producer would");
}

pub async fn seed(pool: &PgPool, email: &str) -> Uuid {
    let id = Uuid::now_v7();
    sqlx::query("INSERT INTO known_users (user_id, email) VALUES ($1, $2)")
        .bind(id)
        .bind(email)
        .execute(pool)
        .await
        .expect("a mirrored row from an earlier run");
    id
}

pub async fn count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM known_users")
        .fetch_one(pool)
        .await
        .expect("count the mirrored rows")
}

pub async fn email_of(pool: &PgPool, id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT email FROM known_users WHERE user_id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .expect("read one mirrored row")
}

pub async fn await_count(pool: &PgPool, want: i64, note: &str) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        if count(pool).await == want {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "{note}");
        tokio::time::sleep(POLL).await;
    }
}

pub async fn await_watermark(pool: &PgPool, mirror: &str, bucket: &str, want: i64, note: &str) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        if watermark(pool, mirror, bucket)
            .await
            .is_some_and(|held| held.0 >= want)
        {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "{note}");
        tokio::time::sleep(POLL).await;
    }
}

pub async fn watermark(pool: &PgPool, mirror: &str, bucket: &str) -> Option<(i64, Option<String>)> {
    sqlx::query_as(
        "SELECT revision, stream_created_at FROM service_engine.mirror_watermark \
         WHERE mirror = $1 AND bucket = $2",
    )
    .bind(mirror)
    .bind(bucket)
    .fetch_optional(pool)
    .await
    .expect("read the mirror watermark")
}

pub async fn last_sequence(fabric: &Nats, bucket: &str) -> u64 {
    fabric
        .bind_stream(&format!("KV_{bucket}"))
        .await
        .expect("the consumed bucket's stream")
        .get_info()
        .await
        .expect("fresh stream metadata")
        .state
        .last_sequence
}
