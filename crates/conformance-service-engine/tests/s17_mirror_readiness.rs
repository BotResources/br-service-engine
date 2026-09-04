use std::sync::Arc;
use std::time::Duration;

use br_util_axum_readiness::{Readiness, ReadinessHandle};
use br_util_nats_fabric::{DEFAULT_MAX_MESSAGES, OutboxRelay, RelayHealth};
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::{
    DIRECTORY_MIRROR, RecordingTransport, SampleDirectory, backfills, directory_mirror_handle,
    known_users, publish_roster, staged_impacts,
};
use service_engine::boot::REASON_MIRRORS;
use service_engine::housekeeping::mirror::{
    MirrorCondition, MirrorSupervisor, MirrorsHealthReceiver,
};
use service_engine::housekeeping::ready::{REASON_RELAY_DEGRADED, ReadinessAssembly};
use service_engine::housekeeping::relay::RelayRuntime;
use service_engine::impact::Impact;
use service_engine::name::{PodId, RelayName};
use service_engine::relays::outbox::FabricOutboxRelay;
use tokio::sync::Notify;
use uuid::Uuid;

const OBSERVED_WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(50);

#[tokio::test]
async fn s17_readiness_is_down_until_the_directory_mirror_converges_and_down_again_when_it_dies() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    nats.provision().await;
    let fabric = nats.fabric().await;

    let mirrored = Uuid::now_v7();
    publish_roster(
        &fabric,
        &SampleDirectory::with_users(&[(mirrored, "one@example.test")]),
    )
    .await;

    let mut supervisor = MirrorSupervisor::new();
    supervisor
        .register(directory_mirror_handle(
            fabric.clone(),
            db.app_pool().clone(),
            Arc::new(RecordingTransport),
        ))
        .expect("the directory mirror registers");
    let relays = RelayRuntime::new(PodId::new("se-mirror-0").expect("a valid pod id"));
    let board = supervisor.health();
    let readiness = ReadinessAssembly::new(ReadinessHandle::ready(), board.clone())
        .with_relays(relays.health());

    assert_eq!(
        readiness.refresh(),
        not_ready(REASON_MIRRORS),
        "a registered mirror holds the gate down before the supervisor has run it once"
    );

    let shutdown = Arc::new(Notify::new());
    let mut tasks = supervisor.start(shutdown.clone());
    assert!(
        tokio::time::timeout(OBSERVED_WITHIN, tasks.converged())
            .await
            .expect("the mirror converged before the deadline"),
        "the supervisor must report converged once the projector reconciled"
    );
    assert_eq!(readiness.refresh(), Readiness::Ready);

    assert_eq!(
        known_users(db.app_pool()).await,
        1,
        "the mirror really projected the roster into known_users"
    );
    assert_eq!(
        backfills(db.app_pool()).await,
        1,
        "the engine backfills its derived state once at adoption"
    );
    let staged = staged_impacts(db.app_pool()).await;
    assert!(
        staged.iter().any(|impact| matches!(
            impact,
            Impact::ForeignChanged { foreign }
                if foreign.namespace().as_str() == "identity.user"
                    && foreign.key().as_str() == mirrored.to_string()
        )),
        "a roster row that changed stages a ForeignChanged through the engine transport, got {staged:?}"
    );

    drop(nats);
    assert!(
        await_condition(&board, |condition| matches!(
            condition,
            MirrorCondition::Restarting { .. }
        ))
        .await,
        "a mirror whose broker died must leave the converged state"
    );
    assert_eq!(
        readiness.refresh(),
        not_ready(REASON_MIRRORS),
        "a dead mirror takes the service back out of rotation"
    );
    assert!(tasks.restarts() >= 1);

    shutdown.notify_waiters();
    db.cleanup().await;
}

#[tokio::test]
async fn s17_a_fabric_relay_that_cannot_publish_takes_a_converged_service_out_of_rotation() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    let fabric = nats.fabric().await;

    let mut tx = db.app_pool().begin().await.expect("the write transaction");
    conformance_service_engine::sample::stage_outbox_row(&mut tx, "degraded").await;
    tx.commit().await.expect("commit");

    let hosted = FabricOutboxRelay::hosting(
        RelayName::from_static("integration_outbox"),
        OutboxRelay::new(db.app_pool().clone(), fabric.clone()),
        DEFAULT_MAX_MESSAGES,
    );
    let hosted_board = hosted.health();
    let mut relays = RelayRuntime::new(PodId::new("se-mirror-0").expect("a valid pod id"));
    relays.register(hosted).expect("the hosted relay registers");
    let readiness =
        ReadinessAssembly::new(ReadinessHandle::ready(), MirrorSupervisor::new().health())
            .with_relays(relays.health());
    assert_eq!(
        readiness.refresh(),
        Readiness::Ready,
        "a relay that has not run yet is on the board as healthy"
    );

    let round = relays.beat(db.app_pool()).await;

    assert_eq!(
        round.failed, 1,
        "publishing onto a broker with no INTEGRATION_EVT stream is a structural failure, not a \
         clean pass"
    );
    assert_eq!(
        readiness.refresh(),
        not_ready(REASON_RELAY_DEGRADED),
        "a relay that cannot publish takes the service out of rotation"
    );
    assert_eq!(
        readiness.verdict(),
        Some(REASON_RELAY_DEGRADED),
        "the /readyz body carries fixed operator copy, never the broker's own message"
    );
    assert_eq!(
        readiness.handle().snapshot(),
        not_ready(REASON_RELAY_DEGRADED),
        "the assembly writes its verdict into the handle the /readyz route serves"
    );

    assert_eq!(
        *hosted_board.borrow(),
        RelayHealth::Healthy,
        "the hosted relay's own board is moved only by the lib's driver loop, which the engine \
         does not run: readiness must read the engine's relay board, not this one"
    );

    db.cleanup().await;
}

fn not_ready(reason: &str) -> Readiness {
    Readiness::NotReady {
        reason: reason.to_string(),
    }
}

async fn await_condition(
    health: &MirrorsHealthReceiver,
    mut ready: impl FnMut(&MirrorCondition) -> bool,
) -> bool {
    let deadline = tokio::time::Instant::now() + OBSERVED_WITHIN;
    while tokio::time::Instant::now() < deadline {
        if health
            .borrow()
            .condition(&DIRECTORY_MIRROR)
            .is_some_and(&mut ready)
        {
            return true;
        }
        tokio::time::sleep(POLL).await;
    }
    false
}
