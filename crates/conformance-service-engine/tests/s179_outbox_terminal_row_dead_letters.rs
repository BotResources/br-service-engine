use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::outbox::row_status;
use conformance_service_engine::sample::stage_outbox_row;
use service_engine::inbound::DeadLetters;
use service_engine::nats::INTEGRATION_EVT;
use service_engine::relays::outbox::OutboxRelay;
use uuid::Uuid;

const MAX_ATTEMPTS: u32 = 5;

#[tokio::test]
async fn s179_a_row_the_reachable_broker_keeps_rejecting_is_dead_lettered_not_abandoned() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    fabric
        .context()
        .create_stream(async_nats::jetstream::stream::Config {
            name: INTEGRATION_EVT.to_string(),
            subjects: vec!["integration.evt.>".to_string()],
            max_message_size: 8,
            ..Default::default()
        })
        .await
        .expect("declare a size-capped event stream that rejects any real envelope");

    let mut tx = pool.begin().await.expect("open the staging transaction");
    let staged = stage_outbox_row(&mut tx, "too-big-to-publish").await;
    tx.commit().await.expect("commit the row");

    let relay =
        OutboxRelay::new(pool.clone(), fabric).with_dead_letters(DeadLetters::new(pool.clone()));

    for attempt in 1..MAX_ATTEMPTS {
        let pass = relay
            .run_once_detailed()
            .await
            .expect("a reachable but rejecting broker never errors the pass");
        assert_eq!(
            pass.retried, 1,
            "attempt {attempt}: a reachable-broker failure counts one bounded retry"
        );
        assert_eq!(
            pass.dead_lettered, 0,
            "attempt {attempt}: still under the bound"
        );
        assert_eq!(
            row_status(&pool, staged).await,
            "PENDING",
            "attempt {attempt}: the row is still retrying, never silently FAILED"
        );
    }

    let last = relay
        .run_once_detailed()
        .await
        .expect("the bound-reaching pass does not error");
    assert_eq!(
        last.dead_lettered, 1,
        "reaching the bound against a reachable broker dead-letters the row"
    );
    assert_eq!(
        row_status(&pool, staged).await,
        "FAILED",
        "the dead-lettered row leaves the pending set"
    );

    let (source, message_id): (String, Uuid) = sqlx::query_as(
        "SELECT source, message_id FROM service_engine.dead_letter WHERE message_id = $1",
    )
    .bind(staged)
    .fetch_one(&pool)
    .await
    .expect("the terminal outbox row is recorded in the one dead-letter table");
    assert_eq!(
        source, "outbox",
        "surfaced under the outbox source, not silently gone"
    );
    assert_eq!(
        message_id, staged,
        "the dead letter carries the row's own message id"
    );

    db.cleanup().await;
}
