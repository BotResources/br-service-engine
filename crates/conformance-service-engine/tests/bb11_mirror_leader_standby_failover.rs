//! Multi-pod mirror (issue #126 §F). Two engine instances of the example service share
//! one Postgres and one NATS. The pod that boots first takes the mirror lease and is the
//! leader: it projects the consumed roster. A second pod converges as a standby — it
//! reaches readiness from the KV bucket alone, with no RPC to the leader. When the leader
//! is rolled out, the standby takes the expired lease and projects a person published
//! *after* the leader died, resuming its watch from an already-current shadow with no reload.
//!
//! That exactly one pod projects while both are alive is proven at the library level by
//! `s102` with per-pod transports; a shared database cannot tell which pod wrote a row, so
//! this black-box scenario proves the binary integration — leader projects, standby reaches
//! readiness, failover resumes the watch — rather than re-proving single-writer exclusivity.
mod blackbox_support;

use std::time::{Duration, Instant};

use blackbox_support::{MirrorBounds, World};
use example_contract::PublishedPerson;
use sqlx::PgPool;
use uuid::Uuid;

/// Short enough that the leader's lease expires inside the failover wait once it stops
/// renewing, rather than after the 30 s production default.
const LEASE: Duration = Duration::from_secs(1);
const BEAT: Duration = Duration::from_millis(200);
const WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(100);

async fn await_projected(pool: &PgPool, id: Uuid, note: &str) {
    let deadline = Instant::now() + WITHIN;
    loop {
        let known: i64 =
            sqlx::query_scalar("SELECT count(*) FROM known_persons WHERE user_id = $1")
                .bind(id)
                .fetch_one(pool)
                .await
                .expect("read the known_persons projection");
        if known == 1 {
            return;
        }
        assert!(Instant::now() < deadline, "{note}");
        tokio::time::sleep(POLL).await;
    }
}

fn person(id: Uuid, local: &str) -> PublishedPerson {
    PublishedPerson {
        id,
        email: format!("{local}@example.test"),
        display_name: local.to_string(),
    }
}

#[tokio::test]
async fn bb11_the_leader_projects_the_standby_converges_and_takes_over_on_lease_loss() {
    // Pod A boots alone, so it takes the mirror lease and leads.
    let mut world = World::start_with_mirror(
        "bb11-leader",
        MirrorBounds {
            lease: LEASE,
            beat: BEAT,
        },
    )
    .await;

    // The leader projects a person published to the roster.
    let first = Uuid::now_v7();
    example_twin::publish_person(&world.nats, &person(first, "one"))
        .await
        .expect("publish the first person to the roster bucket");
    await_projected(
        &world.db.app,
        first,
        "the mirror leader never projected the first person",
    )
    .await;

    // A second pod converges as a standby: spawn_pod_with_mirror gates on /readyz 200, and
    // the mirror is a readiness input, so reaching ready means its shadow is current — it
    // converged from the KV bucket with no RPC to the leader.
    let standby = world
        .spawn_pod_with_mirror(
            "bb11-standby",
            MirrorBounds {
                lease: LEASE,
                beat: BEAT,
            },
        )
        .await;

    // The leader is rolled out from under the fleet.
    world.shutdown_service();

    // A person published after the leader is gone is projected by the standby: it took the
    // expired lease and resumed its watch from its already-current shadow, without a reload.
    let second = Uuid::now_v7();
    example_twin::publish_person(&world.nats, &person(second, "two"))
        .await
        .expect("publish the second person after the leader died");
    await_projected(
        &world.db.app,
        second,
        "the standby never took over the expired lease to project the person published after \
         the leader died",
    )
    .await;

    standby.shutdown().await;
    world.cleanup().await;
}
