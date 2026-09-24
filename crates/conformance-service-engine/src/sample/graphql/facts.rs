use std::sync::Arc;
use std::time::Duration;

use async_graphql::{Context, EmptyMutation, EmptySubscription, Object, Result};
use service_engine::graphql::SliceFragment;
use service_engine::nats::Nats;
use service_engine::{Engine, Readiness, ReadinessHandle, engine_schema};
use uuid::Uuid;

use crate::infra::TestDb;
use crate::sample::graphql::boot::{
    GraphqlService, base_config, free_loopback_addr, sample_prefix,
};
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver, load_registered_tenant};

pub struct FactQueryRoot;

#[Object]
impl FactQueryRoot {
    async fn sample_registered_tenant(&self, ctx: &Context<'_>) -> Result<Option<Uuid>> {
        Ok(ctx.data::<SamplePrincipal>()?.registered_tenant())
    }
}

fn fact_slice() -> SliceFragment {
    SliceFragment::from_claims(
        "registered_tenant",
        vec!["sampleRegisteredTenant".to_string()],
        Vec::new(),
    )
}

pub async fn boot_fact_service(
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
    .expect("the principal-fact engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_principal_fact(load_registered_tenant)
        .expect("register the registered-tenant fact loader");
    engine
        .declare_root_prefix(sample_prefix())
        .expect("declare the sample root prefix");
    engine
        .register_schema_slice(fact_slice())
        .expect("the fact slice owns its root field");

    let readiness = engine.readiness();
    let engine_stop = engine.shutdown_handle();
    let state = Arc::new(engine.graphql_state());
    let schema = engine_schema(
        FactQueryRoot,
        EmptyMutation,
        EmptySubscription,
        state.clone(),
    );
    engine.set_schema_sdl(schema.sdl());
    let app = service_engine::app(schema, state, readiness.clone());

    let handle = tokio::spawn(engine.run_with(app));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    while readiness.snapshot() != Readiness::Ready {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the principal-fact engine never reached readiness UP"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    GraphqlService {
        base_url: format!("http://{addr}"),
        engine_stop,
        handle,
    }
}
