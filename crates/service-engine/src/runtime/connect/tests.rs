use std::time::Duration;

use futures_util::future::BoxFuture;
use sqlx::PgPool;

use crate::accumulator::{ChunkReader, new_registry};
use crate::config::EngineConfig;
use crate::error::EngineError;
use crate::name::{ChannelName, PodId};
use crate::principal::PrincipalResolver;
use crate::registry::RenderRegistry;
use crate::runtime::SessionRuntime;
use crate::session::AttachRequest;
use crate::test_support::TestPrincipal;

use super::*;

struct EchoResolver;

impl PrincipalResolver<TestPrincipal> for EchoResolver {
    fn resolve<'a>(
        &'a self,
        _pg: &'a PgPool,
        current: &'a TestPrincipal,
    ) -> BoxFuture<'a, Result<Option<TestPrincipal>, EngineError>> {
        Box::pin(async move { Ok(Some(current.clone())) })
    }
}

fn runtime() -> std::sync::Arc<SessionRuntime<TestPrincipal>> {
    let pg = PgPool::connect_lazy("postgresql://engine@127.0.0.1:1/engine")
        .expect("a lazy pool never dials");
    let mut registry = RenderRegistry::new();
    registry.register_principal_resolver(EchoResolver);
    SessionRuntime::new(
        EngineConfig::new(
            ChannelName::from_static("service_engine_impact"),
            PodId::from_static("svc-sample-0"),
        ),
        pg.clone(),
        registry,
        ChunkReader::with_registry(pg, new_registry()),
    )
}

#[tokio::test]
async fn an_attach_that_finalizes_after_shutdown_began_is_refused_never_left_live() {
    let render = runtime();
    let guard = render.table.lock().await;

    let attaching = {
        let render = render.clone();
        tokio::spawn(async move {
            render
                .attach(AttachRequest::new(TestPrincipal::new(), Vec::new()))
                .await
        })
    };

    tokio::time::sleep(Duration::from_millis(100)).await;
    render.shutting_down.store(true, Ordering::SeqCst);
    drop(guard);

    let outcome = attaching.await.expect("the attach task joins");
    assert!(
        matches!(outcome, Err(AttachError::ShuttingDown)),
        "an attach whose snapshot finished after shutdown set its flag must be refused, got \
         {outcome:?}"
    );
    assert_eq!(
        render.live_sessions().await,
        0,
        "the refused attach leaves no live session behind on an engine that is shutting down"
    );
}
