use std::time::Duration;

use br_util_axum_readiness::{Readiness, ReadinessHandle};
use conformance_service_engine::infra::TestDb;
use conformance_service_engine::infra::listener::engine_config;
use service_engine::boot::{
    REASON_LISTEN, REASON_MIRRORS, REASON_POSTURE_REFUSED, establish_transport,
};
use service_engine::error::EngineError;
use sqlx::postgres::PgPoolOptions;

const OBSERVED_WITHIN: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(20);

#[tokio::test]
async fn s17_readiness_is_down_until_the_listener_is_established() {
    let db = TestDb::fresh().await;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db.url_as(db.app_role()))
        .await
        .expect("a pool the listener must share with the posture query");
    let held = pool
        .acquire()
        .await
        .expect("hold one of the two connections");
    let readiness = ReadinessHandle::ready();

    let booting = tokio::spawn({
        let pool = pool.clone();
        let config = engine_config("se_s17_impact", "pod-a");
        let readiness = readiness.clone();
        async move { establish_transport(pool, &config, &readiness).await }
    });

    await_reason(&readiness, REASON_LISTEN).await;
    assert_eq!(
        readiness.snapshot(),
        Readiness::NotReady {
            reason: REASON_LISTEN.to_string()
        },
        "while the listener connection is still being taken the gate reads DOWN naming it"
    );

    drop(held);
    let transport = booting
        .await
        .expect("the boot task ran")
        .expect("the runtime posture holds and the listener hears its own probe");

    assert_eq!(
        readiness.snapshot(),
        Readiness::NotReady {
            reason: REASON_MIRRORS.to_string()
        },
        "an established LISTEN is necessary but not sufficient: the gate stays DOWN until the \
         mirrors converge, and boot never flips it UP on its own"
    );
    drop(transport);

    db.cleanup().await;
}

#[tokio::test]
async fn s17_a_refused_boot_leaves_readiness_down_with_the_operator_reason() {
    let db = TestDb::fresh().await;
    let readiness = ReadinessHandle::ready();

    let refused = establish_transport(
        db.superuser_pool().await,
        &engine_config("se_s17_refused", "pod-a"),
        &readiness,
    )
    .await
    .expect_err("a superuser pool is refused at boot");

    match &refused {
        EngineError::Posture(reason) => assert!(
            reason.contains("superuser"),
            "the typed error names the disqualifier, got {reason}"
        ),
        other => panic!("expected a posture refusal, got {other}"),
    }
    assert_eq!(
        readiness.snapshot(),
        Readiness::NotReady {
            reason: REASON_POSTURE_REFUSED.to_string()
        },
        "the gate serves fixed operator copy; the disqualifier travels in the typed error and the \
         log, never in the /readyz body"
    );

    db.cleanup().await;
}

async fn await_reason(readiness: &ReadinessHandle, expected: &str) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        if let Readiness::NotReady { reason } = readiness.snapshot()
            && reason == expected
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the readiness gate never read {expected}"
        );
        tokio::time::sleep(POLL).await;
    }
}
