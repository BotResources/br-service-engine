use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::impact::{ForeignKey, Impact};
use service_engine::mirror::{MirrorHandle, MirrorRun};
use service_engine::name::MirrorName;
use service_engine::nats::{KvBucket, KvEvent, KvKey, KvPrefix, Nats};
use service_engine::transport::ImpactTransport;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

pub const DIRECTORY_MIRROR: MirrorName = MirrorName::from_static("directory");
const USER_PREFIX: &str = "identity/users/";
const USER_NAMESPACE: &str = "identity.user";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SamplePublishedUser {
    pub email: String,
}

pub struct SampleDirectory {
    users: BTreeMap<Uuid, SamplePublishedUser>,
}

impl SampleDirectory {
    pub fn with_users(users: &[(Uuid, &str)]) -> Self {
        Self {
            users: users
                .iter()
                .map(|(id, email)| {
                    (
                        *id,
                        SamplePublishedUser {
                            email: (*email).to_string(),
                        },
                    )
                })
                .collect(),
        }
    }
}

fn user_key(id: Uuid) -> KvKey {
    KvKey::new(format!("{USER_PREFIX}{id}")).expect("a published-user key is valid")
}

fn id_of(key: &KvKey) -> Option<Uuid> {
    key.as_str()
        .strip_prefix(USER_PREFIX)
        .and_then(|raw| Uuid::parse_str(raw).ok())
}

pub async fn publish_roster(nats: &Nats, roster: &SampleDirectory) {
    let bucket = nats
        .published_language::<SamplePublishedUser>()
        .await
        .expect("bind the published-language bucket");
    for (id, user) in &roster.users {
        bucket
            .put(&user_key(*id), user)
            .await
            .expect("publish a roster row the way identity would");
    }
}

async fn apply_user(
    conn: &mut PgConnection,
    transport: &Arc<dyn ImpactTransport>,
    id: Uuid,
    user: Option<&SamplePublishedUser>,
) -> Result<(), EngineError> {
    match user {
        Some(user) => {
            sqlx::query(
                "INSERT INTO known_users (user_id, email) VALUES ($1, $2) \
                 ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email",
            )
            .bind(id)
            .bind(&user.email)
            .execute(&mut *conn)
            .await?;
        }
        None => {
            sqlx::query("DELETE FROM known_users WHERE user_id = $1")
                .bind(id)
                .execute(&mut *conn)
                .await?;
        }
    }
    let foreign = ForeignKey::new(USER_NAMESPACE, &id.to_string())?;
    transport
        .stage_in(conn, &[Impact::ForeignChanged { foreign }])
        .await
}

async fn reconcile(
    nats: &Nats,
    pool: &PgPool,
    transport: &Arc<dyn ImpactTransport>,
) -> Result<(), EngineError> {
    let bucket = published_users(nats).await?;
    let prefix = KvPrefix::new(USER_PREFIX).expect("a valid user prefix");
    let observed = bucket
        .entries(&prefix)
        .await
        .map_err(|error| EngineError::Service(Box::new(error)))?;
    let mut tx = pool.begin().await?;
    let known: Vec<Uuid> = sqlx::query_scalar("SELECT user_id FROM known_users")
        .fetch_all(&mut *tx)
        .await?;
    let present: std::collections::BTreeSet<Uuid> =
        observed.iter().filter_map(|(key, _)| id_of(key)).collect();
    for id in known {
        if !present.contains(&id) {
            apply_user(&mut tx, transport, id, None).await?;
        }
    }
    for (key, user) in &observed {
        if let Some(id) = id_of(key) {
            apply_user(&mut tx, transport, id, Some(user)).await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

async fn watch(
    nats: &Nats,
    pool: &PgPool,
    transport: &Arc<dyn ImpactTransport>,
) -> Result<(), EngineError> {
    let bucket = published_users(nats).await?;
    let mut watch = bucket
        .watch_all()
        .await
        .map_err(|error| EngineError::Service(Box::new(error)))?;
    while let Some(event) = watch.next::<SamplePublishedUser>().await {
        let event = event.map_err(|error| EngineError::Service(Box::new(error)))?;
        let (id, user) = match event {
            KvEvent::Put { key, value, .. } => match id_of(&key) {
                Some(id) => (id, Some(value)),
                None => continue,
            },
            KvEvent::Delete { key, .. } => match id_of(&key) {
                Some(id) => (id, None),
                None => continue,
            },
        };
        let mut tx = pool.begin().await?;
        apply_user(&mut tx, transport, id, user.as_ref()).await?;
        tx.commit().await?;
    }
    Ok(())
}

async fn published_users(nats: &Nats) -> Result<KvBucket<SamplePublishedUser>, EngineError> {
    nats.published_language::<SamplePublishedUser>()
        .await
        .map_err(|error| EngineError::Service(Box::new(error)))
}

pub fn directory_mirror_handle(
    nats: Nats,
    pool: PgPool,
    transport: Arc<dyn ImpactTransport>,
) -> MirrorHandle {
    let reconcile_step = {
        let nats = nats.clone();
        let pool = pool.clone();
        let transport = transport.clone();
        move || {
            let nats = nats.clone();
            let pool = pool.clone();
            let transport = transport.clone();
            Box::pin(async move { reconcile(&nats, &pool, &transport).await }) as MirrorRun
        }
    };
    let watch_step = {
        let nats = nats.clone();
        let pool = pool.clone();
        let transport = transport.clone();
        move || {
            let nats = nats.clone();
            let pool = pool.clone();
            let transport = transport.clone();
            Box::pin(async move { watch(&nats, &pool, &transport).await }) as MirrorRun
        }
    };
    MirrorHandle::new(DIRECTORY_MIRROR, reconcile_step, watch_step).with_backfill(move || {
        let pool = pool.clone();
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
