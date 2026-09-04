use std::collections::BTreeMap;
use std::sync::Arc;

use br_core_directory::{
    DIRECTORY_META_VERSION, DirectoryMeta, PublishedEntity, PublishedGroup,
    PublishedServiceAccount, PublishedUser,
};
use br_util_directory::{DirectoryError, DirectoryProjector, DirectoryPublisher, DirectorySource};
use br_util_nats_fabric::Fabric;
use service_engine::error::EngineError;
use service_engine::housekeeping::mirror::directory::{DirectoryImpactStager, directory_mirror};
use service_engine::mirror::{MirrorHandle, MirrorRun};
use service_engine::name::MirrorName;
use service_engine::transport::ImpactTransport;
use sqlx::PgPool;
use uuid::Uuid;

pub const DIRECTORY_MIRROR: MirrorName = MirrorName::from_static("directory");

pub struct SampleDirectory {
    users: BTreeMap<Uuid, PublishedUser>,
}

impl SampleDirectory {
    pub fn with_users(users: &[(Uuid, &str)]) -> Self {
        Self {
            users: users
                .iter()
                .map(|(id, email)| {
                    (
                        *id,
                        PublishedUser::new((*email).to_string(), None, None, BTreeMap::new())
                            .expect("a published user"),
                    )
                })
                .collect(),
        }
    }
}

#[async_trait::async_trait]
impl DirectorySource for SampleDirectory {
    fn manifest(&self) -> DirectoryMeta {
        DirectoryMeta {
            version: DIRECTORY_META_VERSION,
            entities: vec![PublishedEntity::Users],
        }
    }

    async fn desired_users(&self) -> Result<BTreeMap<Uuid, PublishedUser>, DirectoryError> {
        Ok(self.users.clone())
    }

    async fn desired_groups(&self) -> Result<BTreeMap<Uuid, PublishedGroup>, DirectoryError> {
        Ok(BTreeMap::new())
    }

    async fn desired_service_accounts(
        &self,
    ) -> Result<BTreeMap<Uuid, PublishedServiceAccount>, DirectoryError> {
        Ok(BTreeMap::new())
    }
}

pub async fn publish_roster(fabric: &Fabric, roster: &SampleDirectory) {
    DirectoryPublisher::open(fabric)
        .await
        .expect("open the published-language publisher")
        .reconcile(roster)
        .await
        .expect("publish the roster identity would publish");
}

pub fn directory_mirror_handle(
    fabric: Fabric,
    pool: PgPool,
    transport: Arc<dyn ImpactTransport>,
) -> MirrorHandle {
    let projector = Arc::new(
        DirectoryProjector::new(fabric, pool.clone())
            .with_impact_stager(Arc::new(DirectoryImpactStager::new(transport))),
    );
    directory_mirror(DIRECTORY_MIRROR, projector).with_backfill(move || {
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
