use std::sync::Arc;
use std::time::Duration;

use async_graphql::{Context, EmptyMutation, Object, Result, Subscription};
use futures_util::Stream;
use service_engine::graphql::{SliceFragment, forbidden};
use service_engine::nats::Nats;
use service_engine::{Engine, Readiness, ReadinessHandle, engine_schema};
use tokio::task::JoinHandle;

use crate::infra::TestDb;
use crate::sample::graphql::boot::{GraphqlService, base_config, free_loopback_addr};
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver};

#[derive(Default)]
pub struct ForbiddenQueryRoot;

#[Object]
impl ForbiddenQueryRoot {
    async fn sample_forbidden_peek(&self, _ctx: &Context<'_>) -> Result<bool> {
        Err(forbidden())
    }
}

#[derive(Default)]
pub struct ForbiddenSubscriptionRoot;

#[Subscription]
impl ForbiddenSubscriptionRoot {
    async fn sample_forbidden_stream(
        &self,
        _ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = Result<bool>>> {
        Err::<futures_util::stream::Empty<Result<bool>>, _>(forbidden())
    }
}

fn forbidden_slice() -> SliceFragment {
    SliceFragment::from_claims(
        "forbidden",
        vec!["sampleForbiddenPeek".to_string(), "sampleForbiddenStream".to_string()],
        Vec::new(),
    )
}

pub async fn boot_forbidden_service(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
) -> GraphqlService {
    let addr = free_loopback_addr().await;
    let config = base_config(channel, pod).with_http_addr(addr);

    let mut engine = Engine::<SamplePrincipal>::boot(
        config,
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the forbidden-resolver engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .declare_root_prefix(
            service_engine::graphql::RootPrefix::from_snake("sample")
                .expect("`sample` is a valid root prefix"),
        )
        .expect("declare the sample root prefix");
    engine
        .register_schema_slice(forbidden_slice())
        .expect("the forbidden slice owns its refusing root fields");

    let readiness = engine.readiness();
    let engine_stop = engine.shutdown_handle();
    let state = Arc::new(engine.graphql_state());
    let schema = engine_schema(
        ForbiddenQueryRoot,
        EmptyMutation,
        ForbiddenSubscriptionRoot,
        state.clone(),
    );
    engine.set_schema_sdl(schema.sdl());
    let app = service_engine::app(schema, state, readiness.clone());

    let handle: JoinHandle<std::result::Result<(), service_engine::EngineError>> =
        tokio::spawn(engine.run_with(app));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    while readiness.snapshot() != Readiness::Ready {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the forbidden-resolver engine never reached readiness UP"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    GraphqlService {
        base_url: format!("http://{addr}"),
        engine_stop,
        handle,
    }
}
