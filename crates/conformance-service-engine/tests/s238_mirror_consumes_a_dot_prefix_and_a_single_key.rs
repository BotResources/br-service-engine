mod mirror_support;

use std::collections::HashMap;
use std::sync::Arc;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::{
    RecordingTransport, publish_offer_manifest, replace_known_users,
};
use futures_util::future::BoxFuture;
use mirror_support::{await_count, email_of, persisted_keys, publish, retract};
use serde::{Deserialize, Serialize};
use service_engine::error::EngineError;
use service_engine::mirror::{MirrorReady, Project, Projection};
use service_engine::name::MirrorName;
use service_engine::{Consumed, Mirror, Shadows};
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

struct ItemsAndSettings;

impl Project<Uuid> for ItemsAndSettings {
    type Error = EngineError;

    fn project<'a>(
        &'a self,
        mut cx: Projection<'a>,
        ids: Vec<Uuid>,
    ) -> BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            let mut values: HashMap<Uuid, String> = HashMap::new();
            values.extend(
                cx.shadow::<Settings>()
                    .values()
                    .map(|entry| (entry.id, entry.value.clone())),
            );
            values.extend(
                cx.shadow::<Item>()
                    .values()
                    .map(|entry| (entry.id, entry.value.clone())),
            );
            replace_known_users(&mut cx, ids, &values).await
        })
    }
}

fn ids(shadows: &Shadows) -> Vec<Uuid> {
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
}

fn mirror(name: &'static str) -> MirrorReady<Uuid, ItemsAndSettings> {
    Mirror::new(MirrorName::from_static(name))
        .consume::<Item>()
        .consume::<Settings>()
        .keyed_by(|shadows, _| ids(shadows))
        .project(ItemsAndSettings)
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
async fn s238_a_dot_prefix_and_a_single_key_consume_exactly_their_keys_at_scan_and_at_watch() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let first = Uuid::now_v7();
    let second = Uuid::now_v7();
    let settings = Uuid::now_v7();
    publish(&fabric, &format!("{ITEM_PREFIX}{first}"), first, "item-1").await;
    publish(&fabric, &format!("{ITEM_PREFIX}{second}"), second, "item-2").await;
    publish(&fabric, SETTINGS_KEY, settings, "settings").await;
    publish_offer_manifest(&fabric, ITEM_PREFIX, 1).await;

    let near = Uuid::now_v7();
    publish(&fabric, "catalog.items.near", near, "near-miss-prefix").await;
    publish(&fabric, "catalog.item", near, "the-bare-base").await;
    publish(
        &fabric,
        "catalog.settings.child",
        near,
        "under-the-single-key",
    )
    .await;
    publish(&fabric, "catalog.settingsx", near, "shares-the-characters").await;
    publish(
        &fabric,
        "catalog.settings_extra",
        near,
        "sibling-of-the-key",
    )
    .await;

    let mirror =
        mirror("dot_and_single").build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    mirror.reconcile().await.expect(
        "the scan reads the dot prefix and the single key, and never decodes the sibling \
         manifest as an item",
    );
    assert_eq!(
        mirrored(&pool).await,
        vec!["item-1", "item-2", "settings"],
        "the scan holds every key under catalog.item. and the one key catalog.settings, and \
         nothing that merely shares their characters"
    );

    let watching = tokio::spawn(mirror.watch());

    publish(
        &fabric,
        "catalog.settings.more",
        near,
        "under-the-single-key-live",
    )
    .await;
    publish(&fabric, "catalog.itemz.live", near, "near-miss-prefix-live").await;
    publish_offer_manifest(&fabric, ITEM_PREFIX, 1).await;
    let third = Uuid::now_v7();
    publish(&fabric, &format!("{ITEM_PREFIX}{third}"), third, "item-3").await;
    await_count(
        &pool,
        4,
        "the watch delivers a new key under the dot prefix",
    )
    .await;
    assert_eq!(
        mirrored(&pool).await,
        vec!["item-1", "item-2", "item-3", "settings"],
        "the watch skipped every live key outside both scopes and the rewritten manifest"
    );

    publish(&fabric, SETTINGS_KEY, settings, "settings-v2").await;
    let deadline = tokio::time::Instant::now() + mirror_support::OBSERVED_WITHIN;
    while email_of(&pool, settings).await.as_deref() != Some("settings-v2") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the watch delivers a put on the single key"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    retract(&fabric, SETTINGS_KEY).await;
    await_count(&pool, 3, "the watch delivers a retract of the single key").await;
    assert_eq!(email_of(&pool, settings).await, None);
    assert_eq!(email_of(&pool, near).await, None);

    assert!(
        !watching.is_finished(),
        "the watch is still running: no foreign key or manifest ever failed a decode"
    );
    watching.abort();
    drop(nats);
    db.cleanup().await;
}
