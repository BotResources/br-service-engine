use std::sync::Arc;

use sqlx::PgPool;

use crate::accumulator::{ChunkReader, new_registry};
use crate::config::EngineConfig;
use crate::delta::Delta;
use crate::impact::{Dims, Impact};
use crate::name::{ChannelName, PodId, ProjectorName};
use crate::registry::RenderRegistry;
use crate::render::deliver::{Outgoing, deliver};
use crate::render::pass::{PassContext, run_pass_focused};
use crate::session::SessionId;
use crate::session::live::{Phase, Session};
use crate::session::store::SessionTable;
use crate::session::stream::Outbox;
use crate::test_support::{Assignment, TestPrincipal};
use crate::wire::{KeyBytes, ViewBytes};

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
        dead_letters: None,
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

const RESET_PROJECTOR: ProjectorName = ProjectorName::from_static("assignments");

fn live_session(capacity: usize) -> (Session<TestPrincipal>, Arc<Outbox>) {
    let outbox = Arc::new(Outbox::new(capacity));
    let mut session = Session::pending(
        SessionId::new(),
        TestPrincipal::new(),
        Vec::new(),
        outbox.clone(),
    );
    let _ = session.go_live();
    (session, outbox)
}

fn titled_upsert(key: u32, title: &str) -> Outgoing {
    Outgoing::Upsert {
        projector: RESET_PROJECTOR,
        key: KeyBytes::encode(&key).expect("a key encodes"),
        view: ViewBytes::encode(&title.to_string()).expect("a view encodes"),
        cause: None,
    }
}

fn drain(outbox: &Outbox) -> Vec<Delta> {
    let mut frames = Vec::new();
    while let Some(delta) = outbox.take_front() {
        frames.push(delta);
    }
    frames
}

#[test]
fn a_reset_after_an_unread_delta_carries_the_post_mutation_view_at_a_contiguous_revision() {
    let (mut session, outbox) = live_session(8);

    let first = deliver(&mut session, vec![titled_upsert(1, "beta")]);
    assert_eq!(first.deltas, 1, "the first pass queues one Upsert");
    assert_eq!(session.revision.get(), 1);

    session.reset_pending = true;
    let second = deliver(&mut session, vec![titled_upsert(1, "gamma")]);
    assert_eq!(
        second.deltas, 0,
        "a reset pass discards the freshly rendered upsert into the Reset it delivers instead"
    );
    assert_eq!(second.resets, 1);

    let frames = drain(&outbox);
    assert_eq!(
        frames.len(),
        1,
        "the Reset replaces the delta the client never read"
    );
    match &frames[0] {
        Delta::Reset { views, revision } => {
            assert_eq!(
                revision.get(),
                1,
                "a client that read nothing sees the Reset at the revision the discarded delta \
                 burned, so the sequence stays contiguous"
            );
            assert_eq!(views.len(), 1);
            let title = views[0].view.decode::<String>().expect("a view decodes");
            assert_eq!(
                title, "gamma",
                "the Reset carries the post-mutation view rendered in the reset pass, never the \
                 stale one the queued delta held"
            );
        }
        other => panic!("a reset pass delivers a Reset, got {other:?}"),
    }
}
