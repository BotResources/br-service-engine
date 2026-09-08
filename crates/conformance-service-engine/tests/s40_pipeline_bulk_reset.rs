#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::pipeline::{ImportWidgets, insert_widget};
use conformance_service_engine::sample::principal::SamplePrincipal;
use conformance_service_engine::sample::render::{attach_request, member, next_delta, window};
use conformance_service_engine::sample::{WidgetProjector, boot_pipeline_engine};
use engine_twin::{SOON, await_ready};
use service_engine::delta::Delta;
use uuid::Uuid;

#[tokio::test]
async fn s40_a_bulk_impact_all_resets_a_fixed_keys_window_on_every_pod() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let pool = db.app_pool().clone();

    let tenant = Uuid::now_v7();
    let principal: SamplePrincipal = member(&pool, Uuid::now_v7(), tenant).await;
    let alpha = Uuid::now_v7();
    insert_widget(&pool, alpha, tenant, "alpha").await;

    let engine = boot_pipeline_engine(&db, nats.nats().await, "se_s40", "pod-s40").await;
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
    let opening = match &reset {
        Delta::Reset { views, .. } => views.len(),
        other => panic!("the first frame is a Reset, got {other:?}"),
    };
    assert_eq!(
        opening, 1,
        "the window opens holding the one existing widget"
    );

    let imported = vec![Uuid::now_v7(), Uuid::now_v7()];
    executor
        .run_bulk::<ImportWidgets>(
            principal.clone(),
            ImportWidgets {
                tenant,
                ids: imported.clone(),
            },
        )
        .await
        .expect("the bulk import commits");

    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("the bulk impact_all reaches the fixed Keys window");
    match &delta {
        Delta::Reset { views, .. } => assert_eq!(
            views.len(),
            3,
            "impact_all resets the projector's window, so the two imported rows join the one it \
             already held — a fixed Keys window that would otherwise silently miss the bulk write"
        ),
        other => panic!("impact_all delivers a Reset of the projector's windows, got {other:?}"),
    }

    shutdown.notify_one();
    running.await.expect("join").expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
