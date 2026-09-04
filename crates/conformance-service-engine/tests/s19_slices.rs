use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::note::{Note, NoteKey, NoteProjector};
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::impact::{Dims, Impact};
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s19_slices() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;
    note(&pool, subject, 1, "first note").await;

    let alone = Spy::new();
    let mut first_slice = registry();
    first_slice
        .register_projector(SpyAssignments::new(alone.clone()))
        .expect("the first slice registers alone");
    let engine = runtime(&pool, render_config("pod-one-slice"), first_slice);
    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches with the second slice unregistered");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert_eq!(assignment_ids(reset_views(&reset)), vec![subject]);
    retitle(&pool, subject, "alpha renamed").await;
    let report = engine
        .render(vec![resource(&subject, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(report.deltas, 1);
    assert_eq!(
        engine.registry().names().count(),
        1,
        "the first slice's scenarios pass with the second unregistered"
    );

    let together = Spy::new();
    let mut both_slices = registry();
    both_slices
        .register_projector(SpyAssignments::new(together.clone()))
        .expect("the first slice registers unchanged");
    both_slices
        .register_projector(NoteProjector)
        .expect("adding the second slice touches nothing in the first");
    let engine = runtime(&pool, render_config("pod-two-slices"), both_slices);
    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![
                window(SpyAssignments::NAME, false),
                window(NoteProjector::NAME, false),
            ],
        ))
        .await
        .expect("the session attaches with both slices");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    let projectors: Vec<&service_engine::name::ProjectorName> = reset_views(&reset)
        .iter()
        .map(|view| &view.projector)
        .collect();
    assert!(projectors.contains(&&SpyAssignments::NAME));
    assert!(projectors.contains(&&NoteProjector::NAME));

    retitle(&pool, subject, "alpha renamed again").await;
    let report = engine
        .render(vec![resource(&subject, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 1,
        "the first slice behaves identically with the second registered beside it"
    );
    let delta = next_delta(&mut stream, SOON).await.expect("an Upsert");
    assert_eq!(upserted(&delta).projector, SpyAssignments::NAME);

    db.cleanup().await;
}

#[tokio::test]
async fn s19_the_note_slice_passes_with_the_assignment_slice_unregistered() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;
    note(&pool, subject, 1, "first note").await;

    let mut note_slice = registry();
    note_slice
        .register_projector(NoteProjector)
        .expect("the note slice registers with the assignment slice unregistered");
    let engine = runtime(&pool, render_config("pod-note-slice"), note_slice);
    assert_eq!(
        engine.registry().names().count(),
        1,
        "only the note slice is registered"
    );

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(NoteProjector::NAME, false)],
        ))
        .await
        .expect("the note-only session attaches");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
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
    let report = engine
        .render(vec![
            Impact::resource::<Note>(
                &NoteKey {
                    assignment_id: subject,
                    seq: 2,
                },
                Dims::EMPTY,
            )
            .expect("a note key encodes"),
        ])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 1,
        "a note impact drives the note slice's ordered inbox on its own"
    );
    let delta = next_delta(&mut stream, SOON).await.expect("an Upsert");
    assert_eq!(upserted(&delta).projector, NoteProjector::NAME);

    db.cleanup().await;
}
