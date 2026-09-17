use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use conformance_service_engine::infra::TestDb;
use conformance_service_engine::sample::render::*;
use conformance_service_engine::sample::spy::{Spy, SpyAssignments};
use service_engine::delta::Delta;
use service_engine::inbound::{DeadLetterSource, DeadLetters};
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(2);

async fn render_dead_letters(pool: &sqlx::PgPool) -> Vec<service_engine::inbound::DeadLetter> {
    DeadLetters::new(pool.clone())
        .list(Some(DeadLetterSource::Render), 16)
        .await
        .expect("the ops view lists render dead letters by source")
}

#[tokio::test]
async fn s197_a_projection_failure_at_attach_dead_letters_the_key() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let poison = Arc::new(AtomicBool::new(true));
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()).with_poison_switch(poison.clone()))
        .expect("the projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-attach-poison"), registry);

    let refused = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await;
    assert!(
        refused.is_err(),
        "a document that cannot be projected refuses the attach snapshot",
    );

    let dead = render_dead_letters(&pool).await;
    assert_eq!(
        dead.len(),
        1,
        "the attach-time projection failure is dead-lettered like every other render entry point",
    );
    assert_eq!(dead[0].source, "render");
    assert_eq!(
        dead[0].reaction,
        SpyAssignments::NAME.as_str(),
        "the projector that could not render is the dead-letter's reaction",
    );
    assert!(
        dead[0].subject.contains(&subject.to_string()),
        "the key that could not be projected is the dead-letter's subject",
    );

    poison.store(false, Ordering::Relaxed);
    let mut healthy = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("a new session attaches once the document projects again");
    assert!(
        matches!(
            next_delta(&mut healthy, SOON).await,
            Some(Delta::Reset { .. })
        ),
        "the runtime keeps serving new sessions after an attach-time projection failure",
    );

    db.cleanup().await;
}

#[tokio::test]
async fn s197_a_projection_failure_during_repair_dead_letters_the_key() {
    let db = TestDb::fresh().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let spy = Spy::new();
    let poison = Arc::new(AtomicBool::new(false));
    let mut registry = registry();
    registry
        .register_projector(SpyAssignments::new(spy.clone()).with_poison_switch(poison.clone()))
        .expect("the projector registers on a bound noun");
    let engine = runtime(&pool, render_config("pod-repair-poison"), registry);

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(SpyAssignments::NAME, false)],
        ))
        .await
        .expect("the session attaches on a healthy projection");
    assert!(
        matches!(
            next_delta(&mut stream, SOON).await,
            Some(Delta::Reset { .. })
        ),
        "the session opens before the document turns poison",
    );

    poison.store(true, Ordering::Relaxed);
    engine
        .resnapshot_all()
        .await
        .expect("a reconnect re-snapshots every live session even while one cannot project");

    let dead = render_dead_letters(&pool).await;
    assert_eq!(
        dead.len(),
        1,
        "the repair-time projection failure is dead-lettered like every other render entry point",
    );
    assert_eq!(dead[0].source, "render");
    assert_eq!(
        dead[0].reaction,
        SpyAssignments::NAME.as_str(),
        "the projector that could not render during repair is the dead-letter's reaction",
    );
    assert!(
        dead[0].subject.contains(&subject.to_string()),
        "the key that could not be projected during repair is the dead-letter's subject",
    );

    db.cleanup().await;
}
