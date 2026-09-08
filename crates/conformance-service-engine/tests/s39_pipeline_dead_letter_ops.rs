use std::time::Duration;

use bytes::Bytes;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::boot_pipeline_engine;
use futures_util::StreamExt;
use service_engine::impact::{Impact, TransportEvent};
use service_engine::inbound::{DEAD_LETTER_NOUN, DeadLetterSource, DeadLetters, Incoming, Source};
use uuid::Uuid;

#[tokio::test]
async fn s39_dead_lettering_stages_an_impact_on_the_ops_view() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let engine = boot_pipeline_engine(&db, nats.nats().await, "se_s39", "pod-s39").await;
    let transport = engine.transport_arc();
    let dead_letters = DeadLetters::new(pool.clone()).with_transport(transport.clone());
    let mut listener = transport.listen();

    let poison = Incoming {
        reaction: "sample-poison".to_string(),
        source: Source::Command,
        subject: "integration.cmd.sample.widget.create.v1".to_string(),
        message_id: Uuid::now_v7(),
        sequence: None,
        payload: Bytes::from_static(b"{}"),
        delivered: 9,
    };
    dead_letters
        .record(DeadLetterSource::Reaction, &poison, "unroutable")
        .await
        .expect("the poison message is written to the dead-letter table");

    let dead: i64 = sqlx::query_scalar("SELECT count(*) FROM service_engine.dead_letter")
        .fetch_one(&pool)
        .await
        .expect("count dead letters");
    assert_eq!(dead, 1, "the poison message is recorded");

    let mut saw_ops_impact = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline && !saw_ops_impact {
        match tokio::time::timeout(Duration::from_secs(2), listener.next()).await {
            Ok(Some(Ok(TransportEvent::Impacts(impacts)))) => {
                saw_ops_impact = impacts.iter().any(|impact| {
                    matches!(impact, Impact::ResourceChanged { noun, .. } if *noun == DEAD_LETTER_NOUN)
                });
            }
            Ok(Some(Ok(_))) | Ok(Some(Err(_))) => {}
            Ok(None) | Err(_) => break,
        }
    }
    assert!(
        saw_ops_impact,
        "dead-lettering stages an impact on the ops view, atomically with the dead-letter row",
    );

    drop(nats);
    db.cleanup().await;
}
