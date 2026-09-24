use std::collections::HashMap;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use service_engine::Shadows;
use service_engine::error::EngineError;
use service_engine::mirror::{
    Change, Mirror, MirrorHandle, MirrorReady, MirrorRun, Project, Projection,
};
use service_engine::nats::Nats;
use service_engine::transport::ImpactTransport;
use sqlx::PgPool;
use uuid::Uuid;

use super::known_user::replace_known_users;
use super::{DIRECTORY_MIRROR, REQUIRED_USER_KEY, SamplePublishedUser, id_of, user_key};

pub fn required_key_mirror() -> MirrorReady<Uuid, impl Project<Uuid>> {
    Mirror::new(DIRECTORY_MIRROR)
        .consume::<SamplePublishedUser>()
        .require_key::<SamplePublishedUser>(REQUIRED_USER_KEY)
        .keyed_by(|_shadows: &Shadows, change: &Change| {
            id_of(&change.key).into_iter().collect::<Vec<Uuid>>()
        })
        .project(DirectoryProjection)
        .reconcile_keys(|pool: PgPool| {
            Box::pin(async move {
                let ids: Vec<Uuid> = sqlx::query_scalar("SELECT user_id FROM known_users")
                    .fetch_all(&pool)
                    .await?;
                Ok(ids)
            }) as BoxFuture<'static, Result<Vec<Uuid>, EngineError>>
        })
}

struct DirectoryProjection;

impl Project<Uuid> for DirectoryProjection {
    type Error = EngineError;

    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        ids: Vec<Uuid>,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let emails: HashMap<Uuid, String> = {
                let users = cx.shadow::<SamplePublishedUser>();
                ids.iter()
                    .filter_map(|id| {
                        users
                            .get(&user_key(*id))
                            .map(|user| (*id, user.email.clone()))
                    })
                    .collect()
            };
            replace_known_users(&mut cx, ids, &emails).await
        })
    }
}

pub fn directory_mirror() -> MirrorReady<Uuid, impl Project<Uuid>> {
    Mirror::new(DIRECTORY_MIRROR)
        .consume::<SamplePublishedUser>()
        .keyed_by(|_shadows: &Shadows, change: &Change| {
            id_of(&change.key).into_iter().collect::<Vec<Uuid>>()
        })
        .project(DirectoryProjection)
        .reconcile_keys(|pool: PgPool| {
            Box::pin(async move {
                let ids: Vec<Uuid> = sqlx::query_scalar("SELECT user_id FROM known_users")
                    .fetch_all(&pool)
                    .await?;
                Ok(ids)
            }) as BoxFuture<'static, Result<Vec<Uuid>, EngineError>>
        })
}

pub fn directory_mirror_handle(
    nats: Nats,
    pool: PgPool,
    transport: Arc<dyn ImpactTransport>,
) -> MirrorHandle {
    let backfill_pool = pool.clone();
    directory_mirror()
        .build(nats, pool, transport)
        .with_backfill(move || {
            let pool = backfill_pool.clone();
            Box::pin(async move { backfill(&pool).await }) as MirrorRun
        })
}

async fn backfill(pool: &PgPool) -> Result<(), EngineError> {
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM known_users")
        .fetch_one(pool)
        .await?;
    sqlx::query("INSERT INTO sample_backfill (id, mirror, rows_seen) VALUES ($1, $2, $3)")
        .bind(Uuid::now_v7())
        .bind(DIRECTORY_MIRROR.as_str())
        .bind(rows)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn backfills(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM sample_backfill")
        .fetch_one(pool)
        .await
        .expect("count the one-shot backfills the mirror ran")
}

pub async fn known_users(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM known_users")
        .fetch_one(pool)
        .await
        .expect("count the mirrored roster rows")
}
