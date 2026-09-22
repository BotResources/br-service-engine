use std::sync::Arc;

use service_engine::graphql::SliceFragment;
use service_engine::nats::Nats;
use service_engine::{Engine, ReadinessHandle, engine_schema};
use tokio::net::TcpListener;

use crate::infra::TestDb;
use crate::sample::assignment::AssignmentProjector;
use crate::sample::graphql::boot::{
    base_config, outside_prefix_slice, sample_prefix, widget_slice,
};
use crate::sample::graphql::roots::{MutationRoot, QueryRoot, SubscriptionRoot};
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver};
use crate::sample::widget::WidgetProjector;

pub async fn boot_undeclared_root_field(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
) -> Result<(), service_engine::EngineError> {
    let config = base_config(channel, pod);

    let mut engine = Engine::<SamplePrincipal>::boot(
        config,
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await?;
    engine.register_principal_resolver(SamplePrincipalResolver)?;
    engine.register_projector(WidgetProjector)?;
    engine.register_projector(AssignmentProjector)?;
    engine.declare_root_prefix(sample_prefix())?;
    engine.register_schema_slice(widget_slice())?;

    let readiness = engine.readiness();
    let state = Arc::new(engine.graphql_state());
    let schema = engine_schema(
        QueryRoot::default(),
        MutationRoot,
        SubscriptionRoot,
        state.clone(),
    );
    engine.set_schema_sdl(schema.sdl());
    let app = service_engine::app(schema, state, readiness);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port for the undeclared-root-field boot");
    engine.run_with_listener(listener, app).await
}

pub async fn boot_colliding_slices(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    colliding: SliceFragment,
) -> Result<(), service_engine::EngineError> {
    let config = base_config(channel, pod);

    let mut engine = Engine::<SamplePrincipal>::boot(
        config,
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await?;
    engine.register_principal_resolver(SamplePrincipalResolver)?;
    engine.register_projector(WidgetProjector)?;
    engine.declare_root_prefix(sample_prefix())?;
    engine.register_schema_slice(widget_slice())?;
    engine.register_schema_slice(colliding)?;

    let readiness = engine.readiness();
    let state = Arc::new(engine.graphql_state());
    let schema = engine_schema(
        QueryRoot::default(),
        MutationRoot,
        SubscriptionRoot,
        state.clone(),
    );
    let app = service_engine::app(schema, state, readiness);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port for the colliding-slice boot");
    engine.run_with_listener(listener, app).await
}

pub async fn boot_field_outside_prefix(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
) -> Result<(), service_engine::EngineError> {
    let config = base_config(channel, pod);

    let mut engine = Engine::<SamplePrincipal>::boot(
        config,
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await?;
    engine.register_principal_resolver(SamplePrincipalResolver)?;
    engine.register_projector(WidgetProjector)?;
    engine.declare_root_prefix(sample_prefix())?;
    engine.register_schema_slice(widget_slice())?;
    engine.register_schema_slice(outside_prefix_slice())?;

    let readiness = engine.readiness();
    let state = Arc::new(engine.graphql_state());
    let schema = engine_schema(
        QueryRoot::default(),
        MutationRoot,
        SubscriptionRoot,
        state.clone(),
    );
    let app = service_engine::app(schema, state, readiness);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port for the outside-prefix boot");
    engine.run_with_listener(listener, app).await
}

pub async fn boot_without_prefix(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
) -> Result<(), service_engine::EngineError> {
    let config = base_config(channel, pod);

    let mut engine = Engine::<SamplePrincipal>::boot(
        config,
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await?;
    engine.register_principal_resolver(SamplePrincipalResolver)?;
    engine.register_projector(WidgetProjector)?;
    engine.register_schema_slice(widget_slice())?;

    let readiness = engine.readiness();
    let state = Arc::new(engine.graphql_state());
    let schema = engine_schema(
        QueryRoot::default(),
        MutationRoot,
        SubscriptionRoot,
        state.clone(),
    );
    let app = service_engine::app(schema, state, readiness);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port for the no-prefix boot");
    engine.run_with_listener(listener, app).await
}
