use conformance_service_engine::infra::TestDb;
use service_engine::transport::NOTIFY_PAYLOAD_LIMIT;
use service_engine::transport::payload::within_notify_limit;

async fn pg_notify_accepts(pool: &sqlx::PgPool, bytes: usize) -> bool {
    let payload = "x".repeat(bytes);
    sqlx::query("SELECT pg_notify('se_notify_limit', $1)")
        .bind(payload)
        .execute(pool)
        .await
        .is_ok()
}

#[tokio::test]
async fn s11_a_frame_packed_to_the_maximum_is_accepted_by_pg_notify_and_one_byte_over_is_refused() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool();

    let max = NOTIFY_PAYLOAD_LIMIT - 1;
    assert!(
        within_notify_limit(max, NOTIFY_PAYLOAD_LIMIT),
        "the engine admits a payload of exactly the maximum size"
    );
    assert!(
        pg_notify_accepts(pool, max).await,
        "the real database accepts the maximum payload the engine admits, so the guard is not \
         over-tight"
    );

    assert!(
        !within_notify_limit(NOTIFY_PAYLOAD_LIMIT, NOTIFY_PAYLOAD_LIMIT),
        "the engine refuses a payload at the postgres hard limit"
    );
    assert!(
        !pg_notify_accepts(pool, NOTIFY_PAYLOAD_LIMIT).await,
        "the real database rejects a payload the engine's old guard would have admitted, proving \
         the boundary is exact and not merely masked by header over-estimation"
    );

    db.cleanup().await;
}
