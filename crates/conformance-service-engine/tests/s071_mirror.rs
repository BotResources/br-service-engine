use std::sync::Arc;
use std::time::Duration;

use conformance_service_engine::TestDb;
use conformance_service_engine::infra::TestNats;
use conformance_service_engine::sample::render::{
    attach_request, member, next_delta, registry, removed_key, render_config, reset_views, runtime,
    upserted, window,
};
use conformance_service_engine::sample::{
    KnownUserNoun, RecordingTransport, RosterUserView, RosterUsers, SampleDirectory,
    StagingTransport, directory_mirror, known_users, publish_roster, retract_user, staged_impacts,
};
use service_engine::Nats;
use service_engine::impact::{ForeignKey, Impact};
use sqlx::PgPool;
use uuid::Uuid;

const SOON: Duration = Duration::from_secs(15);
const OBSERVED_WITHIN: Duration = Duration::from_secs(25);
const POLL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn s071_the_boot_full_read_projects_a_prefilled_bucket_before_it_reports_ready() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let user = Uuid::now_v7();
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(user, "roster@example.test")]),
    )
    .await;

    let mirror = directory_mirror().build(fabric.clone(), pool.clone(), StagingTransport::silent());
    mirror
        .reconcile()
        .await
        .expect("the boot full-read converges against a pre-filled bucket");
    assert_eq!(
        known_users(&pool).await,
        1,
        "convergence projects the bucket's whole content into known_users before it reports ready"
    );

    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s071_a_kv_put_lands_in_known_users_and_reaches_a_live_session() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let user = Uuid::now_v7();
    let mirror =
        directory_mirror().build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    let watch = tokio::spawn(mirror.watch());

    establish_email(&fabric, &pool, user, "before@example.test").await;

    let engine = runtime(&pool, render_config("pod-roster"), roster_registry());
    let principal = member(&pool, Uuid::now_v7(), Uuid::now_v7()).await;
    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(RosterUsers::NAME, false)],
        ))
        .await
        .expect("the session attaches over the roster projector");
    let reset = next_delta(&mut stream, SOON).await.expect("a Reset");
    assert_eq!(
        reset_views(&reset)[0]
            .view
            .decode::<RosterUserView>()
            .unwrap()
            .email,
        "before@example.test"
    );

    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(user, "after@example.test")]),
    )
    .await;
    await_state(
        &pool,
        user,
        Some("after@example.test"),
        "the watch never projected the KV put into known_users",
    )
    .await;

    assert!(
        staged_touches(&staged_impacts(&pool).await, user),
        "the mirror stages its own foreign impact for the projected user, observed not assumed"
    );

    let report = engine
        .render(vec![Impact::foreign(
            ForeignKey::new("identity.user", &user.to_string()).unwrap(),
        )])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 1,
        "the mirrored change reaches the one session that shows the user"
    );
    let upsert = next_delta(&mut stream, SOON).await.expect("an Upsert");
    let view = upserted(&upsert).view.decode::<RosterUserView>().unwrap();
    assert_eq!(view.user_id, user);
    assert_eq!(
        view.email, "after@example.test",
        "the live session sees the value the mirror wrote through the direct lane"
    );

    watch.abort();
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s071_a_kv_retract_removes_the_known_row_and_the_session_view() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let user = Uuid::now_v7();
    let mirror =
        directory_mirror().build(fabric.clone(), pool.clone(), Arc::new(RecordingTransport));
    let watch = tokio::spawn(mirror.watch());

    establish_email(&fabric, &pool, user, "present@example.test").await;

    let engine = runtime(&pool, render_config("pod-retract"), roster_registry());
    let principal = member(&pool, Uuid::now_v7(), Uuid::now_v7()).await;
    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(RosterUsers::NAME, false)],
        ))
        .await
        .expect("the session attaches");
    assert_eq!(
        reset_views(&next_delta(&mut stream, SOON).await.expect("a Reset")).len(),
        1
    );

    retract_user(&fabric, user).await;
    await_state(
        &pool,
        user,
        None,
        "the watch never removed the retracted key from known_users",
    )
    .await;
    assert_eq!(
        known_users(&pool).await,
        0,
        "a retract on the bucket deletes the mirrored known_users row"
    );
    assert!(
        staged_touches(&staged_impacts(&pool).await, user),
        "the mirror stages its own foreign impact for the retracted user, observed not assumed"
    );

    let report = engine
        .render(vec![Impact::foreign(
            ForeignKey::new("identity.user", &user.to_string()).unwrap(),
        )])
        .await
        .expect("the pass runs");
    assert_eq!(
        report.deltas, 1,
        "the retract reaches the session as one delta"
    );
    let remove = next_delta(&mut stream, SOON).await.expect("a Remove");
    assert_eq!(
        removed_key(&remove),
        user,
        "the removed key is the retracted user"
    );

    watch.abort();
    drop(nats);
    db.cleanup().await;
}

#[tokio::test]
async fn s071_an_empty_consumed_prefix_holds_readiness_down_and_keeps_known_star() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let kept = Uuid::now_v7();
    sqlx::query("INSERT INTO known_users (user_id, email) VALUES ($1, $2)")
        .bind(kept)
        .bind("kept@example.test")
        .execute(&pool)
        .await
        .expect("a pre-existing mirrored row from an earlier, non-empty run");

    let mirror = directory_mirror().build(fabric.clone(), pool.clone(), StagingTransport::silent());
    let outcome = mirror.reconcile().await;
    let error = outcome.expect_err("an empty consumed prefix must not converge");
    let chain = error_chain(&error);
    assert!(
        chain.contains("identity/users/"),
        "the failure the supervisor surfaces to readiness names the empty prefix, got {chain}"
    );
    assert_eq!(
        known_users(&pool).await,
        1,
        "an empty producer bucket is never a reason to erase the mirror; known_users is kept as is"
    );

    drop(nats);
    db.cleanup().await;
}

fn staged_touches(staged: &[Impact], user: Uuid) -> bool {
    let expected = Impact::foreign(ForeignKey::new("identity.user", &user.to_string()).unwrap());
    staged.contains(&expected)
}

fn roster_registry()
-> service_engine::RenderRegistry<conformance_service_engine::sample::SamplePrincipal> {
    let mut registry = registry();
    registry.bind_noun::<KnownUserNoun>();
    registry
        .register_projector(RosterUsers)
        .expect("the roster projector registers on its bound noun");
    registry
}

fn error_chain(error: &dyn std::error::Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(next) = source {
        message = format!("{message}: {next}");
        source = next.source();
    }
    message
}

async fn current_email(pool: &PgPool, user: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT email FROM known_users WHERE user_id = $1")
        .bind(user)
        .fetch_optional(pool)
        .await
        .expect("read the mirrored row")
}

async fn establish_email(fabric: &Nats, pool: &PgPool, user: Uuid, email: &str) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        publish_roster(fabric, &SampleDirectory::with_users(&[(user, email)])).await;
        let settle = tokio::time::Instant::now() + Duration::from_secs(2);
        while tokio::time::Instant::now() < settle {
            if current_email(pool, user).await.as_deref() == Some(email) {
                return;
            }
            tokio::time::sleep(POLL).await;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the watch never came live to project the roster into known_users"
        );
    }
}

async fn await_state(pool: &PgPool, user: Uuid, want: Option<&str>, note: &str) {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    loop {
        if current_email(pool, user).await.as_deref() == want {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "{note}");
        tokio::time::sleep(POLL).await;
    }
}
