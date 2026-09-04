use std::time::Duration;

use conformance_service_engine::infra::TestDb;
use conformance_service_engine::infra::listener::{
    engine_config, expect_event, next_event, pool_named, terminate_and_wait,
};
use conformance_service_engine::sample::Assignment;
use service_engine::error::TransportError;
use service_engine::impact::{Dims, Impact, TransportEvent};
use service_engine::transport::{ImpactTransport, PgListenNotify};
use uuid::Uuid;

const HEARD_WITHIN: Duration = Duration::from_secs(15);
const LISTENER: &str = "se_s13_listener";
const GARBLED_CHANNEL: &str = "se_s13_garbled_impact";
const FROM_A_NEWER_POD: &str = r#"{"g":"019f8137-e784-7320-87f3-13074aacc4d4","p":0,"n":1,"i":[{"kind":"a_shape_a_newer_pod_stages"}]}"#;

#[tokio::test]
async fn s13_transport_reconnect() {
    let db = TestDb::fresh().await;
    let pool = pool_named(&db, db.app_role(), LISTENER).await;
    let transport = PgListenNotify::connect(pool, &engine_config("se_s13_impact", "pod-a"))
        .await
        .expect("the listener is established");
    let mut stream = transport.listen();

    let before = stage(&db, &transport).await;
    assert_eq!(
        expect_event(&mut stream, HEARD_WITHIN).await,
        TransportEvent::Impacts(vec![before])
    );

    terminate_and_wait(&db, LISTENER).await;
    assert_eq!(
        expect_event(&mut stream, HEARD_WITHIN).await,
        TransportEvent::Reconnected,
        "a killed listener connection is surfaced as Reconnected, never as a silent gap or a \
         normal-looking end of stream"
    );

    let after = stage(&db, &transport).await;
    assert_eq!(
        expect_event(&mut stream, HEARD_WITHIN).await,
        TransportEvent::Impacts(vec![after]),
        "LISTEN resumes on the reconnected connection"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s13_a_payload_this_build_cannot_parse_is_reported_and_repaired_never_silently_fatal() {
    let db = TestDb::fresh().await;
    let pool = pool_named(&db, db.app_role(), "se_s13_garbled").await;
    let transport = PgListenNotify::connect(pool, &engine_config(GARBLED_CHANNEL, "pod-a"))
        .await
        .expect("the listener is established");
    let mut stream = transport.listen();

    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(GARBLED_CHANNEL)
        .bind(FROM_A_NEWER_POD)
        .execute(db.app_pool())
        .await
        .expect("a pod running a newer build stages a frame this one cannot read");

    let reported = next_event(&mut stream, HEARD_WITHIN)
        .await
        .expect("the transport reported the frame rather than going quiet")
        .expect_err("a frame this build cannot parse is an error, never a silent skip");
    assert!(
        matches!(
            reported,
            TransportError::Payload(_) | TransportError::Frame(_)
        ),
        "expected a typed payload failure, got {reported}"
    );

    assert_eq!(
        expect_event(&mut stream, HEARD_WITHIN).await,
        TransportEvent::Reconnected,
        "an unreadable frame is a lost wake, so the transport asks for a Reset instead of ending          the stream and leaving a ready pod blind"
    );

    let after = stage(&db, &transport).await;
    assert_eq!(
        expect_event(&mut stream, HEARD_WITHIN).await,
        TransportEvent::Impacts(vec![after]),
        "the pod keeps rendering after a frame it could not read"
    );

    db.cleanup().await;
}

async fn stage(db: &TestDb, transport: &PgListenNotify) -> Impact {
    let impact = Impact::resource::<Assignment>(&Uuid::now_v7(), Dims::EMPTY).unwrap();
    let mut tx = db
        .app_pool()
        .begin()
        .await
        .expect("open a write transaction");
    transport
        .stage_in(&mut tx, std::slice::from_ref(&impact))
        .await
        .expect("stage the impact");
    tx.commit().await.expect("commit");
    impact
}
