use service_engine::nats::{KvKey, Nats};
use service_engine::{OfferManifest, manifest_key};
use sqlx::PgPool;
use uuid::Uuid;

use super::{REQUIRED_USER_KEY, SampleDirectory, SamplePublishedUser, user_key};

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

pub async fn retract_user(nats: &Nats, id: Uuid) {
    let bucket = nats
        .published_language::<SamplePublishedUser>()
        .await
        .expect("bind the published-language bucket");
    bucket
        .retract(&user_key(id))
        .await
        .expect("retract a roster row the way identity would");
}

pub async fn publish_versioned_user(nats: &Nats, id: Uuid, email: &str, wire: u16) {
    let bucket = nats
        .published_language::<SamplePublishedUser>()
        .await
        .expect("bind the published-language bucket");
    bucket
        .put(
            &user_key(id),
            &SamplePublishedUser {
                email: email.to_string(),
                wire: Some(wire),
            },
        )
        .await
        .expect("publish a versioned roster row");
}

pub async fn publish_required_user(nats: &Nats, email: &str) {
    let bucket = nats
        .published_language::<SamplePublishedUser>()
        .await
        .expect("bind the published-language bucket");
    bucket
        .put(
            &KvKey::new(REQUIRED_USER_KEY).expect("the required key is valid"),
            &SamplePublishedUser {
                email: email.to_string(),
                wire: None,
            },
        )
        .await
        .expect("publish the required configuration key");
}

pub async fn retract_required_user(nats: &Nats) {
    let bucket = nats
        .published_language::<SamplePublishedUser>()
        .await
        .expect("bind the published-language bucket");
    bucket
        .retract(&KvKey::new(REQUIRED_USER_KEY).expect("the required key is valid"))
        .await
        .expect("retract the required configuration key");
}

pub async fn publish_offer_manifest(nats: &Nats, prefix: &str, version: u16) {
    let bucket = nats
        .published_language::<OfferManifest>()
        .await
        .expect("bind the published-language bucket");
    bucket
        .put(
            &manifest_key(prefix).expect("a prefix yields a manifest key"),
            &OfferManifest {
                prefix: prefix.to_string(),
                version,
            },
        )
        .await
        .expect("publish an offer manifest the way a producer would");
}

pub async fn read_offer_manifest(nats: &Nats, prefix: &str) -> Option<OfferManifest> {
    let bucket = nats
        .published_language::<OfferManifest>()
        .await
        .expect("bind the published-language bucket");
    bucket
        .get(&manifest_key(prefix).expect("a prefix yields a manifest key"))
        .await
        .expect("read an offer manifest")
}

pub async fn mirror_dead_letters(pool: &PgPool) -> Vec<(String, String)> {
    sqlx::query_as(
        "SELECT reaction, subject FROM service_engine.dead_letter \
         WHERE source = 'mirror' ORDER BY subject",
    )
    .fetch_all(pool)
    .await
    .expect("read the mirror dead-letter rows")
}
