use std::time::Duration;

use br_util_axum_readiness::{Readiness, ReadinessHandle};
use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::assignment::Assignment;
use conformance_service_engine::sample::render::{
    assignment, attach_request, drain, member, next_delta, reset_views, upserted,
};
use conformance_service_engine::sample::{
    SAMPLE_JOB, SampleDirectory, boot_sample_engine, cron_runs, publish_roster,
};
use service_engine::impact::{Dims, Impact};
use service_engine::name::ProjectorName;
use service_engine::session::{WindowParams, WindowSpec};
use service_engine::transport::ImpactTransport;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

const CHANNEL: &str = "se_assembly_impact";
const READY_WITHIN: Duration = Duration::from_secs(25);
const SOON: Duration = Duration::from_secs(15);
const POLL: Duration = Duration::from_millis(50);

const ASSIGNMENTS: ProjectorName = ProjectorName::from_static("assignments");
const NOTES: ProjectorName = ProjectorName::from_static("notes");

#[tokio::test]
async fn the_whole_engine_assembles_boots_serves_and_shuts_down_through_its_public_api() {
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

    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let subject = assignment(&pool, home, "alpha").await;

    let engine = boot_sample_engine(&db, fabric.clone(), CHANNEL, "pod-assembly").await;
    let readiness = engine.readiness();
    let render = engine.render();
    let transport = engine.transport_arc();
    let shutdown = engine.shutdown_handle();

    assert_eq!(
        readiness.snapshot(),
        Readiness::NotReady {
            reason: service_engine::boot::REASON_MIRRORS.to_string()
        },
        "an engine that has booted its transport but not yet run its mirrors reads DOWN"
    );

    let mut stream = engine
        .attach(attach_request(
            &principal,
            vec![window(ASSIGNMENTS), window(NOTES)],
        ))
        .await
        .expect("a session attaches before the engine loop starts");

    let running = tokio::spawn(engine.run());

    await_ready(&readiness).await;

    let opening = next_delta(&mut stream, SOON)
        .await
        .expect("the attached session opens with a Reset once the loop drains its held impacts");
    assert_eq!(opening.revision().get(), 1);
    let projectors: Vec<&ProjectorName> = reset_views(&opening)
        .iter()
        .map(|view| &view.projector)
        .collect();
    assert!(
        projectors.contains(&&ASSIGNMENTS),
        "the Reset carries the first slice's view: {projectors:?}"
    );

    stage_retitle(
        &db,
        transport.as_ref(),
        subject,
        "alpha through the whole engine",
    )
    .await;
    let upsert = next_delta(&mut stream, SOON)
        .await
        .expect("a staged impact round-trips through the engine's own LISTEN into a delta");
    let title = upserted(&upsert)
        .view
        .decode::<serde_json::Value>()
        .unwrap()["title"]
        .as_str()
        .expect("a title")
        .to_string();
    assert_eq!(title, "alpha through the whole engine");

    stage_relay_row(&pool).await;
    await_relay_drained(&pool).await;
    await_cron_ran(&pool).await;

    assert_eq!(render.live_sessions().await, 1);

    shutdown.notify_one();
    running
        .await
        .expect("the engine task joins")
        .expect("run returns Ok on a clean shutdown");
    drop(drain(&mut stream).await);
    assert!(
        next_delta(&mut stream, SOON).await.is_none(),
        "the stream ends explicitly when the engine shuts down"
    );

    drop(nats);
    db.cleanup().await;
}

fn window(projector: ProjectorName) -> WindowSpec {
    WindowSpec::new(projector, WindowParams::none(), false)
}

async fn stage_retitle(db: &TestDb, transport: &dyn ImpactTransport, id: Uuid, title: &str) {
    let mut tx = db.app_pool().begin().await.expect("a write transaction");
    retitle_tx(&mut tx, id, title).await;
    let impact =
        Impact::resource::<Assignment>(&id, Dims::EMPTY).expect("an assignment key encodes");
    transport
        .stage_in(&mut tx, std::slice::from_ref(&impact))
        .await
        .expect("stage the impact in the same transaction as the write");
    tx.commit().await.expect("commit the gesture");
}

async fn retitle_tx(tx: &mut PgConnection, id: Uuid, title: &str) {
    sqlx::query("UPDATE sample_assignment SET title = $1 WHERE id = $2")
        .bind(title)
        .bind(id)
        .execute(&mut *tx)
        .await
        .expect("retitle inside the write transaction");
}

async fn stage_relay_row(pool: &PgPool) {
    sqlx::query("INSERT INTO sample_relay_row (id) VALUES ($1)")
        .bind(Uuid::now_v7())
        .execute(pool)
        .await
        .expect("stage an outbox-style row the RowClaim relay must drain on the beat");
}

async fn await_ready(readiness: &ReadinessHandle) {
    let deadline = tokio::time::Instant::now() + READY_WITHIN;
    loop {
        if readiness.snapshot() == Readiness::Ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "readiness never rose to UP: mirrors, LISTEN and relays did not all compose green"
        );
        tokio::time::sleep(POLL).await;
    }
}

async fn await_relay_drained(pool: &PgPool) {
    let deadline = tokio::time::Instant::now() + SOON;
    loop {
        let pending: i64 =
            sqlx::query_scalar("SELECT count(*) FROM sample_relay_row WHERE claimed_at IS NULL")
                .fetch_one(pool)
                .await
                .expect("count unclaimed rows");
        if pending == 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the beat never drove the RowClaim relay to claim the staged row"
        );
        tokio::time::sleep(POLL).await;
    }
}

async fn await_cron_ran(pool: &PgPool) {
    let deadline = tokio::time::Instant::now() + SOON;
    loop {
        if cron_runs(pool, SAMPLE_JOB).await >= 1 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the beat never drove the cron job through a leader slot"
        );
        tokio::time::sleep(POLL).await;
    }
}
