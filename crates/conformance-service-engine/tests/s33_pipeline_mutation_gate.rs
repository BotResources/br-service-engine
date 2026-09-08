#[allow(dead_code)]
mod engine_twin;

use std::collections::BTreeMap;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::pipeline::{CloseWidget, insert_widget};
use conformance_service_engine::sample::principal::SamplePrincipal;
use conformance_service_engine::sample::render::{
    attach_request, member, next_delta, reset_views, upserted, window,
};
use conformance_service_engine::sample::{WidgetProjector, boot_pipeline_engine};
use engine_twin::{SOON, await_ready};
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
struct GateDto {
    allowed: bool,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WidgetDto {
    closed: bool,
    affordances: BTreeMap<String, GateDto>,
}

#[tokio::test]
async fn s33_a_mutation_gates_deny_and_allow_through_one_pipeline_and_the_impact_reaches_the_session()
 {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal: SamplePrincipal = member(&pool, Uuid::now_v7(), tenant).await;
    let widget = Uuid::now_v7();
    insert_widget(&pool, widget, tenant, "alpha").await;

    let engine = boot_pipeline_engine(&db, nats.nats().await, "se_s33", "pod-s33").await;
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let executor = engine.mutation_executor();

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(WidgetProjector::NAME, false)],
        ))
        .await
        .expect("the widget session attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");
    let opening = reset_views(&reset);
    let opened: WidgetDto = opening[0].view.decode().expect("the opening view decodes");
    assert!(!opened.closed);
    assert!(
        opened.affordances["close"].allowed,
        "an open widget can close"
    );

    executor
        .run::<CloseWidget>(principal.clone(), CloseWidget { id: widget })
        .await
        .expect("the gate allows closing an open widget, so the pipeline commits");

    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("committing the close flips the affordance and reaches the session");
    let flipped: WidgetDto = upserted(&delta)
        .view
        .decode()
        .expect("the flipped view decodes");
    assert!(
        flipped.closed,
        "the committed state reaches the session, not the mutation response"
    );
    assert!(!flipped.affordances["close"].allowed);
    assert_eq!(
        flipped.affordances["close"].reason.as_deref(),
        Some("already_closed"),
    );

    let refused = executor
        .run::<CloseWidget>(principal.clone(), CloseWidget { id: widget })
        .await
        .expect_err("the same gate that blocks the affordance refuses the mutation");
    assert_eq!(
        refused.code(),
        Some("already_closed"),
        "the mutation refuses with the affordance's own reason code, from one gate function",
    );

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
