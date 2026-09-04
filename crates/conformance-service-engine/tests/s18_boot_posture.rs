use conformance_service_engine::infra::TestDb;
use service_engine::boot::assert_posture;
use service_engine::error::EngineError;

#[tokio::test]
async fn s18_boot_posture() {
    let db = TestDb::fresh().await;

    assert_posture(db.app_pool())
        .await
        .expect("the low-privilege runtime role is the sanctioned posture");

    assert_refused(
        assert_posture(db.owner_pool()).await,
        "owns schema service_engine",
    );

    let superuser = db.superuser_pool().await;
    assert_refused(assert_posture(&superuser).await, "is a superuser");
    superuser.close().await;

    let bypass = db
        .pool_as(db.bypass_role())
        .await
        .expect("a pool on the BYPASSRLS role");
    assert_refused(assert_posture(&bypass).await, "rolbypassrls");
    bypass.close().await;

    let member = db
        .pool_as(db.member_role())
        .await
        .expect("a pool on the NOINHERIT role granted the owner role");
    assert_refused(
        assert_posture(&member).await,
        "owns schema service_engine, or may assume the role that does",
    );
    member.close().await;

    db.cleanup().await;
}

fn assert_refused(outcome: Result<(), EngineError>, expected: &str) {
    match outcome {
        Ok(()) => panic!("the posture assertion accepted a role it must refuse ({expected})"),
        Err(EngineError::Posture(reason)) => assert!(
            reason.contains(expected),
            "the refusal names the disqualifier: expected {expected}, got {reason}"
        ),
        Err(other) => panic!("expected a posture refusal, got {other}"),
    }
}
