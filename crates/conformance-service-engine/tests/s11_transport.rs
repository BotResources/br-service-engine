use std::time::Duration;

use conformance_service_engine::infra::TestDb;
use conformance_service_engine::infra::listener::{
    engine_config, expect_event, next_event, pool_named,
};
use conformance_service_engine::sample::Assignment;
use service_engine::impact::{Dims, Impact, TransportEvent};
use service_engine::name::NounName;
use service_engine::transport::payload::encode;
use service_engine::transport::{ImpactTransport, NOTIFY_PAYLOAD_LIMIT, PgListenNotify};
use service_engine::wire::Noun;
use uuid::Uuid;

const HEARD_WITHIN: Duration = Duration::from_secs(10);
const SILENCE: Duration = Duration::from_millis(300);

struct Padded;

impl Noun for Padded {
    type Key = String;
    const NAME: NounName = NounName::from_static("padded");
}

#[tokio::test]
async fn s11_the_staging_pod_hears_itself() {
    let db = TestDb::fresh().await;
    let pool = pool_named(&db, db.app_role(), "se_s11_self").await;
    let transport = PgListenNotify::connect(pool, &engine_config("se_s11_self", "pod-a"))
        .await
        .expect("the listener is established and hears its own probe");
    let mut stream = transport.listen();

    let impact = Impact::resource::<Assignment>(&Uuid::now_v7(), Dims::EMPTY).unwrap();
    let mut tx = db
        .app_pool()
        .begin()
        .await
        .expect("open a write transaction");
    transport
        .stage_in(&mut tx, std::slice::from_ref(&impact))
        .await
        .expect("stage the impact inside the caller's transaction");
    assert!(
        next_event(&mut stream, SILENCE).await.is_none(),
        "a staged impact must not leave before its transaction commits"
    );
    tx.commit().await.expect("commit");

    assert_eq!(
        expect_event(&mut stream, HEARD_WITHIN).await,
        TransportEvent::Impacts(vec![impact]),
        "the staging pod hears itself, with no local shortcut and no echo suppression"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s11_a_list_above_the_notify_limit_is_split_and_fully_heard() {
    let db = TestDb::fresh().await;
    let pool = pool_named(&db, db.app_role(), "se_s11_split").await;
    let transport = PgListenNotify::connect(pool, &engine_config("se_s11_split", "pod-a"))
        .await
        .expect("the listener is established");
    let mut stream = transport.listen();

    let staged: Vec<Impact> = (0..300)
        .map(|i| {
            let key = format!("{i:0>200}");
            Impact::resource::<Padded>(&key, Dims::EMPTY).unwrap()
        })
        .collect();
    let frames = encode(&staged, NOTIFY_PAYLOAD_LIMIT).expect("the list encodes");
    assert!(
        frames.len() > 1,
        "the fixture must exceed one notification to prove the split"
    );
    for frame in &frames {
        assert!(frame.len() <= NOTIFY_PAYLOAD_LIMIT);
    }

    let mut tx = db
        .app_pool()
        .begin()
        .await
        .expect("open a write transaction");
    transport
        .stage_in(&mut tx, &staged)
        .await
        .expect("stage the oversized list");
    tx.commit().await.expect("commit");

    assert_eq!(
        expect_event(&mut stream, HEARD_WITHIN).await,
        TransportEvent::Impacts(staged),
        "every part of a split list is reassembled before it reaches a subscriber"
    );

    db.cleanup().await;
}
