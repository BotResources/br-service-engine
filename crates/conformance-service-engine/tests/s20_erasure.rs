use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::note::{NoteProjector, NoteView};
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::error::DecodeError;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s20_erasure() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;
    note(&pool, subject, 1, "first note").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()))
        .expect("the assignment projector registers");
    registry
        .register_projector(NoteProjector)
        .expect("the note projector registers");
    let engine = runtime(&pool, render_config("pod-erasure"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![
                window(SpyAssignments::NAME, false),
                window(NoteProjector::NAME, false),
            ],
        ))
        .await
        .expect("the session attaches to both projectors");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    let views = reset_views(&reset);
    assert_eq!(views.len(), 2, "one erased view per projector");

    let assignment_view = views
        .iter()
        .find(|view| view.projector == SpyAssignments::NAME)
        .expect("the assignment view rides the same stream");
    let note_view = views
        .iter()
        .find(|view| view.projector == NoteProjector::NAME)
        .expect("the note view rides the same stream");

    let probe = SpyAssignments::new(Spy::new());
    let (key, decoded) = assignment_view
        .decode_from::<SpyAssignments>(&probe)
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
                if expected == &NoteProjector::NAME && found == &SpyAssignments::NAME
        ),
        "the refusal names both projectors, got {refusal:?}"
    );

    db.cleanup().await;
}
