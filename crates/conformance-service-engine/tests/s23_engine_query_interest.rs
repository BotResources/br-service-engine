#[allow(dead_code)]
mod engine_twin;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::assignment::{lifecycle_dim, title_dim};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::note::{Note, NoteKey};
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments, WindowMode};
use engine_twin::{SILENCE, SOON, await_ready, spy_engine, stage};
use service_engine::impact::{Deps, ForeignKey, Impact};
use service_engine::principal::Principal;
use uuid::Uuid;

const CHANNEL: &str = "se_s23_engine";

#[tokio::test]
async fn s23_engine_only_impacts_inside_the_query_interest_re_evaluate_the_window() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let engine = spy_engine(
        &db,
        fabric,
        engine_config(CHANNEL, "pod-s23"),
        SpyAssignments::new(spy.clone())
            .with_window(WindowMode::LiveQuery)
            .with_dims(title_dim()),
    )
    .await;
    let readiness = engine.readiness();
    let transport = engine.transport_arc();
    let shutdown = engine.shutdown_handle();

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;
    next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");

    spy.reset();
    stage(
        &pool,
        transport.as_ref(),
        &[
            Impact::resource::<Note>(
                &NoteKey {
                    assignment_id: subject,
                    seq: 1,
                },
                title_dim(),
            )
            .expect("a note key encodes"),
            resource(&subject, lifecycle_dim()),
            Impact::principal_facts(principal.id(), Deps::bit(3).unwrap()),
            Impact::foreign(
                ForeignKey::new("identity.group", &subject.to_string())
                    .expect("a valid foreign key"),
            ),
        ],
    )
    .await;
    assert!(
        next_delta(&mut stream, SILENCE).await.is_none(),
        "an impact outside the Query window's Interest yields nothing"
    );
    assert_eq!(
        spy.populates(),
        0,
        "an impact outside the Interest leaves populate uncalled"
    );

    spy.reset();
    stage(
        &pool,
        transport.as_ref(),
        &[resource(&subject, title_dim())],
    )
    .await;
    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("a ResourceChanged inside the Interest re-evaluates the window");
    assert_eq!(upserted(&delta).key.decode::<Uuid>().unwrap(), subject);
    assert_eq!(
        spy.populates(),
        1,
        "an impact inside the Interest re-evaluates the window exactly once"
    );

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
