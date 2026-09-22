use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::pipeline::{RelabelWidget, insert_widget, widget_label};
use conformance_service_engine::sample::principal::SamplePrincipal;
use conformance_service_engine::sample::render::member;
use conformance_service_engine::sample::{
    boot_policy_engine, boot_transition_policy_engine, boot_unhonoured_seam_engine,
};
use service_engine::error::EngineError;
use uuid::Uuid;

#[tokio::test]
async fn s233_a_post_save_policy_refuses_the_write_with_its_reason_and_commits_nothing() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal: SamplePrincipal = member(&pool, Uuid::now_v7(), tenant).await;
    let widget = Uuid::now_v7();
    insert_widget(&pool, widget, tenant, "clean").await;

    let engine = boot_policy_engine(&db, nats.nats().await, "se_s233", "pod-s233").await;
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());

    executor
        .run::<RelabelWidget>(
            principal.clone(),
            RelabelWidget {
                id: widget,
                label: "still-clean".to_string(),
            },
        )
        .await
        .expect("the policy allows a clean label");
    assert_eq!(
        widget_label(&pool, widget).await.as_deref(),
        Some("still-clean")
    );

    let refused = executor
        .run::<RelabelWidget>(
            principal.clone(),
            RelabelWidget {
                id: widget,
                label: "BREACH-open".to_string(),
            },
        )
        .await
        .expect_err("the post-save policy refuses a breach label");
    assert_eq!(
        refused.code(),
        Some("BREACH_LABEL"),
        "the refusal carries the policy's reason code, not a generic engine error"
    );
    assert_eq!(
        widget_label(&pool, widget).await.as_deref(),
        Some("still-clean"),
        "the refused save rolled back, so the row keeps its previous label"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s233_a_transition_aware_policy_reads_the_prior_image_and_refuses_a_no_op_save() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal: SamplePrincipal = member(&pool, Uuid::now_v7(), tenant).await;
    let widget = Uuid::now_v7();
    insert_widget(&pool, widget, tenant, "a").await;

    let engine =
        boot_transition_policy_engine(&db, nats.nats().await, "se_s233t", "pod-s233t").await;
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();
    let running = tokio::spawn(engine.run());

    executor
        .run::<RelabelWidget>(
            principal.clone(),
            RelabelWidget {
                id: widget,
                label: "b".to_string(),
            },
        )
        .await
        .expect("a real transition a->b is allowed");
    assert_eq!(widget_label(&pool, widget).await.as_deref(), Some("b"));

    let refused = executor
        .run::<RelabelWidget>(
            principal.clone(),
            RelabelWidget {
                id: widget,
                label: "b".to_string(),
            },
        )
        .await
        .expect_err("a save that does not change the label is refused");
    assert_eq!(
        refused.code(),
        Some("UNCHANGED_LABEL"),
        "the policy compared prior (the committed 'b') to next and saw no transition"
    );
    assert_eq!(
        widget_label(&pool, widget).await.as_deref(),
        Some("b"),
        "the refused no-op save rolled back and the committed label stands"
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s233_a_declared_subjection_with_no_registered_policy_fails_the_boot_seam_check() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;

    let engine = boot_unhonoured_seam_engine(&db, nats.nats().await, "se_s233c", "pod-s233c").await;
    let error = engine
        .run()
        .await
        .expect_err("run must refuse to serve with an unhonoured seam");
    assert!(
        matches!(error, EngineError::UnhonouredSeam { .. }),
        "boot fails with the unhonoured-seam error, not silently: {error:?}"
    );

    drop(nats);
    db.cleanup().await;
}
