use std::sync::Arc;
use std::time::Duration;

use async_graphql::{EmptyMutation, EmptySubscription, ObjectType};
use service_engine::config::EngineConfig;
use service_engine::graphql::SliceFragment;
use service_engine::nats::Nats;
use service_engine::{Engine, EngineError, Readiness, ReadinessHandle, engine_schema};

use crate::infra::TestDb;
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver};

use super::boot::{GraphqlService, free_loopback_addr, sample_prefix};

pub(super) async fn boot_query_service<Q, R>(
    db: &TestDb,
    nats: Nats,
    config: EngineConfig,
    root: Q,
    slice: SliceFragment,
    register: R,
) -> GraphqlService
where
    Q: ObjectType + 'static,
    R: FnOnce(&mut Engine<SamplePrincipal>) -> Result<(), EngineError>,
{
    let addr = free_loopback_addr().await;
    let mut engine = Engine::<SamplePrincipal>::boot(
        config.with_http_addr(addr),
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await
    .expect("the query engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    register(&mut engine).expect("the service's own registrations succeed");
    engine
        .declare_root_prefix(sample_prefix())
        .expect("declare the sample root prefix");
    engine
        .register_schema_slice(slice)
        .expect("the query slice owns its root fields and types");

    let readiness = engine.readiness();
    let engine_stop = engine.shutdown_handle();
    let state = Arc::new(engine.graphql_state());
    let schema = engine_schema(root, EmptyMutation, EmptySubscription, state.clone());
    engine.set_schema_sdl(schema.sdl());
    let app = service_engine::app(schema, state, readiness.clone());
    let handle = tokio::spawn(engine.run_with(app));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    while readiness.snapshot() != Readiness::Ready {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the query engine never reached readiness UP"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    GraphqlService {
        base_url: format!("http://{addr}"),
        engine_stop,
        handle,
    }
}
