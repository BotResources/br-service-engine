use std::time::Duration;

use conformance_service_engine::infra::{TestDb, TestNats};
use conformance_service_engine::sample::assignment::Assignment;
use conformance_service_engine::sample::boot_render_engine;
use conformance_service_engine::sample::render::{
    assignment, attach_request, member, next_delta, reset_views, upserted,
};
use service_engine::delta::Delta;
use service_engine::impact::{Dims, Impact};
use service_engine::name::ProjectorName;
use service_engine::session::{WindowParams, WindowSpec};
use service_engine::transport::ImpactTransport;
use uuid::Uuid;

const CHANNEL: &str = "se_s11_two_engines";
const SOON: Duration = Duration::from_secs(15);
const ASSIGNMENTS: ProjectorName = ProjectorName::from_static("assignments");

async fn stage(transport: &dyn ImpactTransport, pool: &sqlx::PgPool, id: Uuid, title: &str) {
    let mut tx = pool.begin().await.expect("open a write transaction");
    sqlx::query("UPDATE sample_assignment SET title = $1 WHERE id = $2")
        .bind(title)
        .bind(id)
        .execute(&mut *tx)
        .await
        .expect("retitle inside the write transaction");
    let impact =
        Impact::resource::<Assignment>(&id, Dims::EMPTY).expect("an assignment key encodes");
    transport
        .stage_in(&mut tx, std::slice::from_ref(&impact))
        .await
        .expect("stage the impact inside the same transaction");
    tx.commit().await.expect("commit");
}

fn title_of(delta: &Delta) -> String {
    upserted(delta).view.decode::<serde_json::Value>().unwrap()["title"]
        .as_str()
        .expect("a title")
        .to_string()
}

fn window() -> WindowSpec {
    WindowSpec::new(ASSIGNMENTS, WindowParams::none(), false)
}

#[tokio::test]
async fn s11_two_engines_on_one_database_each_deliver_to_their_own_sessions() {
    let db = TestDb::fresh().await;
    let nats = TestNats::spawn().await;
    let fabric = nats.fabric().await;
    let pool = db.app_pool().clone();
    let home = Uuid::now_v7();
    let principal = member(&pool, Uuid::now_v7(), home).await;
    let id = assignment(&pool, home, "alpha").await;

    let engine_a = boot_render_engine(&db, fabric.clone(), CHANNEL, "pod-a").await;
    let engine_b = boot_render_engine(&db, fabric.clone(), CHANNEL, "pod-b").await;
    let render_a = engine_a.render();
    let render_b = engine_b.render();
    let transport_a = engine_a.transport_arc();
    let transport_b = engine_b.transport_arc();
    let stop_a = engine_a.shutdown_handle();
    let stop_b = engine_b.shutdown_handle();

    let mut stream_a = engine_a
        .attach(attach_request(&principal, vec![window()]))
        .await
        .expect("a session attaches on engine A");
    let mut stream_b = engine_b
        .attach(attach_request(&principal, vec![window()]))
        .await
        .expect("a session attaches on engine B");

    let running_a = tokio::spawn(engine_a.run());
    let running_b = tokio::spawn(engine_b.run());

    for stream in [&mut stream_a, &mut stream_b] {
        let opening = next_delta(stream, SOON)
            .await
            .expect("each session opens with its own Reset");
        assert_eq!(opening.revision().get(), 1);
        assert_eq!(
            reset_views(&opening)
                .iter()
                .map(|v| v.key.decode::<Uuid>().unwrap())
                .collect::<Vec<_>>(),
            vec![id]
        );
    }

    stage(transport_a.as_ref(), &pool, id, "staged by pod a").await;
    let heard_a = next_delta(&mut stream_a, SOON)
        .await
        .expect("the staging engine hears itself and renders its own session");
    let heard_b = next_delta(&mut stream_b, SOON)
        .await
        .expect("the other engine hears the same notification and renders its own session");
    assert_eq!(heard_a.revision().get(), 2);
    assert_eq!(heard_b.revision().get(), 2);
    assert_eq!(title_of(&heard_a), "staged by pod a");
    assert_eq!(title_of(&heard_b), "staged by pod a");

    stage(transport_b.as_ref(), &pool, id, "staged by pod b").await;
    let heard_a = next_delta(&mut stream_a, SOON)
        .await
        .expect("engine A renders an impact engine B staged");
    let heard_b = next_delta(&mut stream_b, SOON)
        .await
        .expect("engine B hears itself");
    assert_eq!(heard_a.revision().get(), 3);
    assert_eq!(heard_b.revision().get(), 3);
    assert_eq!(title_of(&heard_a), "staged by pod b");
    assert_eq!(title_of(&heard_b), "staged by pod b");

    assert_eq!(
        render_a.live_sessions().await,
        1,
        "an engine knows only its own sessions"
    );
    assert_eq!(render_b.live_sessions().await, 1);
    assert_eq!(render_a.metrics().deltas, 2);
    assert_eq!(render_b.metrics().deltas, 2);

    stop_a.notify_one();
    stop_b.notify_one();
    running_a
        .await
        .expect("engine A's task joins")
        .expect("engine A shuts down cleanly");
    running_b
        .await
        .expect("engine B's task joins")
        .expect("engine B shuts down cleanly");
    assert!(
        next_delta(&mut stream_a, SOON).await.is_none(),
        "the stream ends explicitly when the engine shuts down"
    );
    assert!(next_delta(&mut stream_b, SOON).await.is_none());

    drop(nats);
    db.cleanup().await;
}
