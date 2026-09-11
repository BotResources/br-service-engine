use std::time::Duration;

use conformance_service_engine::infra::listener::engine_config;
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::assignment::{Assignment, AssignmentProjector};
use conformance_service_engine::sample::principal::{SamplePrincipalResolver, SampleRls};
use conformance_service_engine::sample::render::{
    assignment, attach_request, member, next_delta, window,
};
use futures_util::StreamExt;
use service_engine::Engine;
use service_engine::delta::Delta;
use service_engine::{Readiness, ReadinessHandle};
use uuid::Uuid;

const READY_WITHIN: Duration = Duration::from_secs(25);
const STILL_LIVE_WITHIN: Duration = Duration::from_millis(700);
const ENDS_WITHIN: Duration = Duration::from_secs(8);
const POLL: Duration = Duration::from_millis(50);

const SESSION_TTL: Duration = Duration::from_secs(1);
const SESSION_MAX_AGE: Duration = Duration::from_secs(2);

#[tokio::test]
async fn s078_a_live_session_is_ended_for_a_fresh_passport_once_it_reaches_session_max_age() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    let fabric = nats.nats().await;
    let pool = db.app_pool().clone();

    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    assignment(&pool, home, "alpha").await;

    let config = engine_config("se_s078_max_age", "pod-max-age")
        .with_session_ttl(SESSION_TTL)
        .with_session_max_age(SESSION_MAX_AGE);
    assert!(
        config.session_max_age > config.session_ttl,
        "session_max_age must outlast the idle ttl, so this scenario ends a live session by age, \
         never by idleness"
    );

    let readiness = ReadinessHandle::ready();
    let mut engine = Engine::boot(config, pool.clone(), fabric, readiness.clone())
        .await
        .expect("the engine boots");
    engine.bind_noun::<Assignment>().expect("bind the noun");
    engine.register_rls(SampleRls).expect("register the rls");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the resolver");
    engine
        .register_projector(AssignmentProjector)
        .expect("register the projector");

    let render = engine.render();
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let mut stream = render
        .attach(attach_request(
            &principal,
            vec![window(AssignmentProjector::NAME, false)],
        ))
        .await
        .expect("the session attaches while the engine is live");
    let opening = next_delta(&mut stream, ENDS_WITHIN)
        .await
        .expect("the opening Reset arrives");
    assert!(
        matches!(opening, Delta::Reset { .. }),
        "a fresh session opens with a Reset, got {opening:?}"
    );

    let too_early = tokio::time::timeout(STILL_LIVE_WITHIN, stream.next()).await;
    assert!(
        too_early.is_err(),
        "a session well inside its max age is still live, so its stream neither ends nor is force-\
         reset; only reaching session_max_age may end it"
    );

    loop {
        match tokio::time::timeout(ENDS_WITHIN, stream.next()).await {
            Ok(Some(_)) => continue,
            Ok(None) => break,
            Err(_) => panic!(
                "a live session outlived session_max_age: the engine never ended it, so a revoked \
                 scope could keep an affordance allowed past the bound"
            ),
        }
    }

    running.abort();
    drop(nats);
    db.cleanup().await;
}

async fn await_ready(readiness: &ReadinessHandle) {
    let deadline = tokio::time::Instant::now() + READY_WITHIN;
    loop {
        if readiness.snapshot() == Readiness::Ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "readiness never rose to UP"
        );
        tokio::time::sleep(POLL).await;
    }
}
