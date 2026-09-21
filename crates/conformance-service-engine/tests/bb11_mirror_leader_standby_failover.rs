mod blackbox_support;

use std::time::{Duration, Instant};

use blackbox_support::{MirrorBounds, World};
use example_contract::PublishedPerson;
use sqlx::PgPool;
use uuid::Uuid;

const LEASE: Duration = Duration::from_secs(1);
const BEAT: Duration = Duration::from_millis(200);
const WITHIN: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(100);

async fn await_projected(pool: &PgPool, id: Uuid, note: &str) {
    let deadline = Instant::now() + WITHIN;
    loop {
        let known: i64 =
            sqlx::query_scalar("SELECT count(*) FROM roster.known_persons WHERE user_id = $1")
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

async fn mirror_leader_gauge(base_url: &str) -> Option<f64> {
    let text = reqwest::Client::new()
        .get(format!("{base_url}/metrics"))
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;
    text.lines()
        .filter(|line| {
            line.starts_with("service_engine_leader{") && line.contains("kind=\"mirror\"")
        })
        .filter_map(|line| line.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()))
        .fold(None, |acc, value| {
            Some(acc.map_or(value, |m: f64| m.max(value)))
        })
}

async fn await_mirror_leader(base_url: &str, want: f64, note: &str) {
    let deadline = Instant::now() + WITHIN;
    loop {
        if mirror_leader_gauge(base_url).await == Some(want) {
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
    let mut world = World::start_with_mirror(
        "bb11-leader",
        MirrorBounds {
            lease: LEASE,
            beat: BEAT,
        },
    )
    .await;

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
    await_mirror_leader(
        world.base_url(),
        1.0,
        "the mirror leader never reported itself as the leader of its loop on /metrics",
    )
    .await;

    let standby = world
        .spawn_pod_with_mirror(
            "bb11-standby",
            MirrorBounds {
                lease: LEASE,
                beat: BEAT,
            },
        )
        .await;
    await_mirror_leader(
        standby.base_url(),
        0.0,
        "the standby reported itself as leader on /metrics while the leader was still alive",
    )
    .await;

    world.shutdown_service();

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
    await_mirror_leader(
        standby.base_url(),
        1.0,
        "the standby never reported taking leadership on /metrics after the leader died",
    )
    .await;

    standby.shutdown().await;
    world.cleanup().await;
}
