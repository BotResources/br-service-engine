#[allow(dead_code)]
mod engine_twin;

use br_util_axum_readiness::ReadinessHandle;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::SamplePrincipal;
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::note::{Note, NoteKey, NoteProjector};
use conformance_service_engine::sample::principal::SamplePrincipalResolver;
use conformance_service_engine::sample::render::*;
use engine_twin::{SOON, await_ready, stage};
use service_engine::Engine;
use service_engine::impact::{Dims, Impact};
use uuid::Uuid;

const CHANNEL: &str = "se_s19_engine";

#[tokio::test]
async fn s19_engine_the_note_slice_runs_end_to_end_with_the_assignment_slice_unregistered() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;
    note(&pool, subject, 1, "first note").await;

    let mut engine = Engine::<SamplePrincipal>::boot(
        engine_config(CHANNEL, "pod-s19"),
        pool.clone(),
        fabric,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the engine boots under the low-privilege app role");
    engine.bind_noun::<Note>().expect("bind the note noun");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(NoteProjector)
        .expect("the note slice registers with the assignment slice absent");
    let readiness = engine.readiness();
    let transport = engine.transport_arc();
    let shutdown = engine.shutdown_handle();

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(NoteProjector::NAME, false)],
        ))
        .await
        .expect("the note-only session attaches");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");
    let opened: Vec<&service_engine::name::ProjectorName> = reset_views(&reset)
        .iter()
        .map(|view| &view.projector)
        .collect();
    assert_eq!(
        opened,
        vec![&NoteProjector::NAME],
        "the note slice enumerates its own inbox with no assignment slice present"
    );

    note(&pool, subject, 2, "second note").await;
    stage(
        &pool,
        transport.as_ref(),
        &[Impact::resource::<Note>(
            &NoteKey {
                assignment_id: subject,
                seq: 2,
            },
            Dims::EMPTY,
        )
        .expect("a note key encodes")],
    )
    .await;
    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("a note impact drives the note slice's ordered inbox on its own");
    assert_eq!(upserted(&delta).projector, NoteProjector::NAME);

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
