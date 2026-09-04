use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::sample::assignment::{lifecycle_dim, title_dim};
use conformance_service_engine::sample::note::{Note, NoteKey};
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{
    FOREIGN_NAMESPACE, Spy, SpyAssignments, WindowMode, membership_dep,
};
use service_engine::impact::{Deps, Dims, ForeignKey, Impact};
use service_engine::principal::Principal;
use service_engine::wire::Noun;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s23_query_interest() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(
            SpyAssignments::new(spy.clone())
                .with_window(WindowMode::LiveQuery)
                .with_dims(title_dim()),
        )
        .expect("the spy projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-query"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    next_delta(&mut stream, SOON).await.expect("a Reset");

    spy.reset();
    let report = engine
        .render(vec![
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
        ])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.populates, 0,
        "an impact outside a Query window's Interest leaves populate uncalled"
    );
    assert_eq!(spy.populates(), 0);
    assert_eq!(report.deltas, 0);

    spy.reset();
    let report = engine
        .render(vec![resource(&subject, title_dim())])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.populates, 1,
        "a ResourceChanged inside the Interest re-evaluates the window"
    );
    let delta = next_delta(&mut stream, SOON)
        .await
        .expect("the key the predicate accepted joins the window");
    assert_eq!(upserted(&delta).key.decode::<Uuid>().unwrap(), subject);

    spy.reset();
    let report = engine
        .render(vec![Impact::foreign(
            ForeignKey::new(FOREIGN_NAMESPACE, &subject.to_string()).expect("a valid foreign key"),
        )])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.populates, 1,
        "a ForeignChanged inside the Interest re-evaluates the window"
    );

    spy.reset();
    let report = engine
        .render(vec![Impact::principal_facts(
            principal.id(),
            membership_dep(),
        )])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.populates, 1,
        "a PrincipalFactsChanged inside the Interest re-evaluates the window"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s23_a_query_window_is_re_evaluated_by_a_noun_its_interest_declares_but_its_projector_does_not_render()
 {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(
            SpyAssignments::new(spy.clone())
                .with_window(WindowMode::LiveQuery)
                .also_interested_in(Note::NAME),
        )
        .expect("the spy projector registers on the only noun it renders");
    let engine = runtime(&pool, render_config("pod-interest"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    next_delta(&mut stream, SOON).await.expect("a Reset");

    spy.reset();
    let report = engine
        .render(vec![
            Impact::resource::<Note>(
                &NoteKey {
                    assignment_id: subject,
                    seq: 1,
                },
                Dims::EMPTY,
            )
            .expect("a note key encodes"),
        ])
        .await
        .expect("the pass runs");
    assert!(
        report.faults.is_empty(),
        "a noun the projector does not render must not fault the window, got {:?}",
        report.faults
    );
    assert_eq!(
        report.populates, 1,
        "a Query window is woken by every noun its Interest names, not only the ones its \
         projector renders"
    );
    assert_eq!(spy.populates(), 1);

    db.cleanup().await;
}

#[tokio::test]
async fn s23_query_membership_grows_on_a_foreign_and_on_a_principal_impact() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(
            SpyAssignments::new(spy.clone()).with_window(WindowMode::MembershipQuery),
        )
        .expect("the membership-query projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-membership"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let opening = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert!(
        reset_views(&opening).is_empty(),
        "the tenant has no assignments yet, so the membership query opens empty"
    );

    let alpha = assignment(&pool, home, "alpha").await;
    let report = engine
        .render(vec![Impact::foreign(
            ForeignKey::new(FOREIGN_NAMESPACE, &Uuid::now_v7().to_string())
                .expect("a valid foreign key"),
        )])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.populates, 1,
        "a ForeignChanged inside the Interest re-evaluates the window"
    );
    let grew = next_delta(&mut stream, SOON)
        .await
        .expect("the newly visible key populates into the window");
    assert_eq!(
        upserted(&grew).key.decode::<Uuid>().unwrap(),
        alpha,
        "populate, not the predicate, added the key on a foreign impact"
    );

    let beta = assignment(&pool, home, "beta").await;
    let report = engine
        .render(vec![Impact::principal_facts(
            principal.id(),
            membership_dep(),
        )])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.populates, 1,
        "a PrincipalFactsChanged inside the Interest re-evaluates the window"
    );
    let grew = next_delta(&mut stream, SOON)
        .await
        .expect("the second key populates into the window");
    assert_eq!(
        upserted(&grew).key.decode::<Uuid>().unwrap(),
        beta,
        "populate added the key on a principal impact, so membership can change on both axes"
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s23_a_key_the_predicate_discovered_is_dropped_when_the_re_evaluation_disowns_it() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let mut registry = registry();
    registry
        .register_projector(
            SpyAssignments::new(spy.clone()).with_window(WindowMode::QueryThenEmpty),
        )
        .expect("the projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-disowned"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    let first = next_delta(&mut stream, SOON)
        .await
        .expect("a session opens with its Reset");
    assert!(
        reset_views(&first).is_empty(),
        "a Query window opens holding only what it has discovered, which is nothing yet"
    );

    engine
        .render(vec![resource(&subject, Dims::EMPTY)])
        .await
        .expect("the pass runs");
    assert!(
        next_delta(&mut stream, Duration::from_millis(300))
            .await
            .is_none(),
        "a key the predicate discovered but the re-evaluation disowns is held by no window, \
         so it is never rendered into a view no Remove could ever clear"
    );

    db.cleanup().await;
}
