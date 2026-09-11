use std::sync::Arc;

use sqlx::PgPool;

use crate::accumulator::{ChunkReader, new_registry};
use crate::config::EngineConfig;
use crate::impact::{Dims, Impact};
use crate::name::{ChannelName, PodId};
use crate::registry::RenderRegistry;
use crate::render::pass::{PassContext, run_pass_focused};
use crate::session::SessionId;
use crate::session::live::{Phase, Session};
use crate::session::store::SessionTable;
use crate::session::stream::Outbox;
use crate::test_support::{Assignment, TestPrincipal};

fn held_len(session: &Session<TestPrincipal>) -> usize {
    match &session.phase {
        Phase::Pending { held, .. } => held.len(),
        _ => panic!("the session under test must still be pending"),
    }
}

#[tokio::test]
async fn a_focused_replay_does_not_re_hold_its_impacts_for_other_pending_sessions() {
    let pg = PgPool::connect_lazy("postgresql://engine@127.0.0.1:1/engine")
        .expect("a lazy pool never dials");
    let registry = RenderRegistry::<TestPrincipal>::new();
    let chunks = ChunkReader::with_registry(pg.clone(), new_registry());
    let config = EngineConfig::new(
        ChannelName::new("service_engine_focus").expect("a valid channel"),
        PodId::new("svc-focus-0").expect("a valid pod id"),
    );
    let ctx = PassContext {
        pg: &pg,
        registry: &registry,
        chunks: &chunks,
        config: &config,
    };

    let mut going_live = Session::pending(
        SessionId::new(),
        TestPrincipal::new(),
        Vec::new(),
        Arc::new(Outbox::new(4)),
    );
    let focus = going_live.id;
    let _ = going_live.go_live();

    let mut pending = Session::pending(
        SessionId::new(),
        TestPrincipal::new(),
        Vec::new(),
        Arc::new(Outbox::new(4)),
    );
    let other = pending.id;
    let replayed =
        vec![Impact::resource::<Assignment>(&uuid::Uuid::now_v7(), Dims::EMPTY).expect("a key")];
    pending.hold(&replayed, config.max_held_impacts);
    let before = held_len(&pending);

    let mut table = SessionTable::new();
    table.insert(going_live);
    table.insert(pending);

    run_pass_focused(&ctx, &mut table, &replayed, Some(focus))
        .await
        .expect("a focused replay pass runs without touching the database");

    assert_eq!(
        held_len(
            table
                .get(other)
                .expect("the other session is still pending")
        ),
        before,
        "a focused go-live replay must not re-hold its impacts for an unrelated pending \
         session, which would double-count toward the held cap and force an avoidable Reset"
    );
}
