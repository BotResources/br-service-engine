mod mirror_support;

use std::sync::Arc;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::{
    RecordingTransport, mirror_dead_letters, publish_offer_manifest,
};
use futures_util::future::BoxFuture;
use mirror_support::{persisted_keys, publish};
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::mirror::{MirrorReady, Project, Projection};
use service_engine::name::MirrorName;
use service_engine::nats::KvKey;
use service_engine::{Consumed, Mirror, OfferManifest, Shadows};
use sqlx::PgPool;
use uuid::Uuid;

const ITEM_PREFIX: &str = "catalog.item.";
const SETTINGS_KEY: &str = "catalog.settings";

#[derive(Clone, Serialize, Deserialize)]
struct Item {
    id: Uuid,
    value: String,
}

impl Consumed for Item {
    const PREFIX: &'static str = ITEM_PREFIX;
}

#[derive(Clone, Serialize, Deserialize)]
struct Settings {
    id: Uuid,
    value: String,
}

impl Consumed for Settings {
    const PREFIX: &'static str = SETTINGS_KEY;
}

struct Rows;

impl Project<Uuid> for Rows {
    type Error = EngineError;

    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        id: Uuid,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let value = cx
                .shadow::<Item>()
                .values()
                .find(|item| item.id == id)
                .map(|item| item.value.clone())
                .or_else(|| {
                    cx.shadow::<Settings>()
                        .values()
                        .find(|settings| settings.id == id)
                        .map(|settings| settings.value.clone())
                });
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

fn mirror(name: &'static str) -> MirrorReady<Uuid, Rows> {
    Mirror::new(MirrorName::from_static(name))
        .consume::<Item>()
        .consume::<Settings>()
        .keyed_by(|shadows: &Shadows, _| {
            shadows
                .shadow::<Item>()
                .values()
                .map(|item| item.id)
                .chain(
                    shadows
                        .shadow::<Settings>()
                        .values()
                        .map(|settings| settings.id),
                )
                .collect()
        })
        .project(Rows)
        .reconcile_keys(persisted_keys)
}

async fn mirrored(pool: &PgPool) -> Vec<String> {
    let mut values: Vec<String> = sqlx::query_scalar("SELECT email FROM known_users")
        .fetch_all(pool)
        .await
        .expect("read the mirrored rows");
    values.sort();
    values
}

#[tokio::test]
async fn s239_a_dot_prefix_is_judged_against_its_sibling_manifest_and_a_single_key_is_not() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let item = Uuid::now_v7();
    let settings = Uuid::now_v7();
    publish(&fabric, &format!("{ITEM_PREFIX}{item}"), item, "item").await;
    publish(&fabric, SETTINGS_KEY, settings, "settings").await;

    publish_offer_manifest(&fabric, ITEM_PREFIX, 2).await;
    fabric
        .published_language::<OfferManifest>()
        .await
        .expect("bind the published-language bucket")
        .put(
            &KvKey::new(format!("{SETTINGS_KEY}_manifest")).expect("a valid key"),
            &OfferManifest {
                prefix: format!("{SETTINGS_KEY}."),
                version: 9,
            },
        )
        .await
        .expect("an unrelated manifest beside the single key");

    let mirror =
        mirror("dot_manifest").build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    mirror
        .reconcile()
        .await
        .expect("a manifest mismatch never fails the reconcile");

    assert_eq!(
        mirrored(&pool).await,
        vec!["settings"],
        "the dot prefix is withheld by its v2 manifest, and the single key is judged against no \
         manifest, so it projects"
    );
    assert_eq!(
        mirror_dead_letters(&pool).await,
        vec![("dot_manifest".to_string(), ITEM_PREFIX.to_string())],
        "one dead letter for the rejected dot prefix, none for the single key"
    );

    publish_offer_manifest(&fabric, ITEM_PREFIX, 1).await;
    mirror
        .reconcile()
        .await
        .expect("the next scan reads the matching manifest");
    assert_eq!(
        mirrored(&pool).await,
        vec!["item", "settings"],
        "a manifest at the consumer's version under the sibling key admits the dot prefix"
    );

    drop(nats);
    db.cleanup().await;
}
