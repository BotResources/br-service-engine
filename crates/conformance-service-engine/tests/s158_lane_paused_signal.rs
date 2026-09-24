#[allow(dead_code)]
mod engine_twin;

use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::engine::engine_config;
use conformance_service_engine::sample::principal::SamplePrincipalResolver;
use conformance_service_engine::sample::render::{attach_request, member, next_delta};
use conformance_service_engine::sample::{Typing, typing_window};
use engine_twin::{SOON, await_ready};
use futures_util::StreamExt;
use service_engine::{Delta, Engine, Lane, LaneNotice, Readiness, ReadinessHandle};
use uuid::Uuid;

const CHANNEL: &str = "se_s158_lane_signal";
const SERVICE: &str = "s158lane";
const GRACE: Duration = Duration::from_secs(30);

async fn next_notice(
    stream: &mut (impl futures_util::Stream<Item = LaneNotice> + Unpin),
) -> LaneNotice {
    tokio::time::timeout(Duration::from_secs(15), stream.next())
        .await
        .expect("a lane notice arrives before the deadline")
        .expect("the lane-notice stream never ends while the pod runs")
}

#[tokio::test]
async fn s158_a_paused_lane_tells_every_session_before_grace_and_resumes_with_a_reset() {
    let db = TestDb::fresh().await;
    let mut nats = TestNats::spawn().await;
    nats.provision().await;
    nats.provision_presence(SERVICE, Duration::from_secs(30))
        .await;

    let config = engine_config(CHANNEL, "pod-s158")
        .with_service(SERVICE)
        .with_nats_grace(GRACE)
        .with_beat(Duration::from_millis(200));
    let mut engine = Engine::boot(
        config,
        db.app_pool().clone(),
        nats.nats().await,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the presence pod boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_presence::<Typing>(Duration::from_secs(30))
        .expect("register the typing presence lane");

    let readiness = engine.readiness();
    let shutdown = engine.shutdown_handle();
    let render = engine.render();
    let mut notices = Box::pin(engine.lane_notices());
    let running = tokio::spawn(engine.run());
    await_ready(&readiness).await;

    let pool = db.app_pool().clone();
    let room = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), Uuid::now_v7()).await;
    let mut session = render
        .attach(attach_request(&principal, vec![typing_window(room)]))
        .await
        .expect("a session attaches to the presence lane");
    let opening = next_delta(&mut session, SOON)
        .await
        .expect("the opening Reset");
    let opening_revision = opening.revision().get();
    assert!(matches!(opening, Delta::Reset { .. }));

    nats.stop();

    let paused = next_notice(&mut notices).await;
    assert_eq!(
        paused,
        LaneNotice::Paused(vec![Lane::Presence]),
        "the presence lane pauses from the first beat that observes the broker unreachable and \
         every session that watches it is told so on its subscription",
    );
    assert_eq!(
        readiness.snapshot(),
        Readiness::Ready,
        "the lane paused while the outage is still well within nats_grace, so readiness — which \
         nats_grace alone governs — has not fallen: had the pause waited for nats_grace, this \
         notice could not arrive while the pod is still UP",
    );

    nats.restart().await;

    let resumed = next_notice(&mut notices).await;
    assert_eq!(
        resumed,
        LaneNotice::Resumed(vec![Lane::Presence]),
        "on return the lane resumes and the same session is told",
    );

    let reset = next_delta(&mut session, SOON)
        .await
        .expect("the session receives a Reset on return");
    assert!(
        matches!(reset, Delta::Reset { .. }),
        "a Reset on return reaches the session because a pause was signalled",
    );
    assert!(
        reset.revision().get() > opening_revision,
        "the Reset advances the contiguous per-session revision rather than rewinding it; the \
         out-of-band lane notice carried no revision of its own",
    );

    assert_eq!(
        readiness.snapshot(),
        Readiness::Ready,
        "the outage stayed within nats_grace throughout, so readiness never left UP even as the \
         lane paused and resumed",
    );

    shutdown.notify_one();
    running
        .await
        .expect("the pod task joins")
        .expect("the pod run returns Ok after the lane resumed");
    drop(nats);
    db.cleanup().await;
}
