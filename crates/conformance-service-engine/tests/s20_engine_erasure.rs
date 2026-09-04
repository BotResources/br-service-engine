#[allow(dead_code)]
mod engine_twin;

use br_util_axum_readiness::ReadinessHandle;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::SamplePrincipal;
use conformance_service_engine::sample::assignment::{Assignment, AssignmentProjector};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::note::{Note, NoteProjector, NoteView};
use conformance_service_engine::sample::principal::{SamplePrincipalResolver, SampleRls};
use conformance_service_engine::sample::render::*;
use engine_twin::{SOON, await_ready};
use service_engine::Engine;
use service_engine::error::DecodeError;
use uuid::Uuid;

const CHANNEL: &str = "se_s20_engine";

#[tokio::test]
async fn s20_engine_both_views_ride_one_stream_and_each_recovers_its_typed_view() {
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
        engine_config(CHANNEL, "pod-s20"),
        pool.clone(),
        fabric,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the engine boots under the low-privilege app role");
    engine.bind_noun::<Assignment>().expect("bind assignment");
    engine.bind_noun::<Note>().expect("bind note");
    engine.register_rls(SampleRls).expect("register RLS");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the resolver");
    engine
        .register_projector(AssignmentProjector)
        .expect("register the assignment projector");
    engine
        .register_projector(NoteProjector)
        .expect("register the note projector");
    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![
                window(AssignmentProjector::NAME, false),
                window(NoteProjector::NAME, false),
            ],
        ))
        .await
        .expect("the session attaches to both projectors");
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let reset = next_delta(&mut stream, SOON)
        .await
        .expect("the opening Reset");
    let views = reset_views(&reset);
    assert_eq!(
        views.len(),
        2,
        "one erased view per projector rides the same stream"
    );

    let assignment_view = views
        .iter()
        .find(|view| view.projector == AssignmentProjector::NAME)
        .expect("the assignment view is present");
    let note_view = views
        .iter()
        .find(|view| view.projector == NoteProjector::NAME)
        .expect("the note view is present");

    let (key, decoded) = assignment_view
        .decode_from::<AssignmentProjector>(&AssignmentProjector)
        .expect("the erased assignment view recovers its typed key and view");
    assert_eq!(key, subject);
    assert_eq!(decoded.title, "alpha");

    let (note_key, decoded_note): (_, NoteView) = note_view
        .decode_from::<NoteProjector>(&NoteProjector)
        .expect("the erased note view recovers its typed key and view");
    assert_eq!(note_key.assignment_id, subject);
    assert_eq!(decoded_note.body, "first note");

    let refusal = assignment_view
        .decode_from::<NoteProjector>(&NoteProjector)
        .expect_err("decoding a view with the wrong projector fails typed");
    assert!(
        matches!(
            refusal,
            DecodeError::Projector { ref expected, ref found }
                if expected == &NoteProjector::NAME && found == &AssignmentProjector::NAME
        ),
        "the refusal names both projectors, got {refusal:?}"
    );

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok");
    drop(nats);
    db.cleanup().await;
}
