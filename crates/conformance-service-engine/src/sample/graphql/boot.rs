use std::sync::Arc;
use std::time::Duration;

use service_engine::config::EngineConfig;
use service_engine::graphql::SliceFragment;
use service_engine::name::{ChannelName, PodId};
use service_engine::nats::Nats;
use service_engine::{Engine, engine_schema};
use service_engine::{Readiness, ReadinessHandle};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::infra::TestDb;
use crate::sample::assignment::AssignmentProjector;
use crate::sample::graphql::rls::{RlsAssignmentProjector, RlsQueryRoot};
use crate::sample::graphql::roots::{MutationRoot, QueryRoot, SubscriptionRoot};
use crate::sample::pipeline::{CloseWidget, MintSecret, close_widget, mint_secret};
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver, SampleRls};
use crate::sample::widget::WidgetProjector;

fn claims(slice: &'static str, root_fields: &[&str], types: &[&str]) -> SliceFragment {
    SliceFragment::from_claims(
        slice,
        root_fields.iter().map(|f| f.to_string()).collect(),
        types.iter().map(|t| t.to_string()).collect(),
    )
}

fn widget_slice() -> SliceFragment {
    claims(
        "widget",
        &["widget", "closeWidget", "mintSecret", "widgets"],
        &["WidgetView"],
    )
}

fn assignment_slice() -> SliceFragment {
    claims(
        "assignment",
        &["assignment", "assignments"],
        &["AssignmentView"],
    )
}

fn rls_assignment_slice() -> SliceFragment {
    claims("rls_assignment", &["rlsAssignment"], &["AssignmentView"])
}

pub fn root_field_collision() -> SliceFragment {
    claims("shadow", &["widget"], &["ShadowView"])
}

pub fn type_collision() -> SliceFragment {
    claims("shadow", &["shadow"], &["WidgetView"])
}

pub struct GraphqlService {
    pub base_url: String,
    engine_stop: Arc<Notify>,
    handle: JoinHandle<Result<(), service_engine::EngineError>>,
}

impl GraphqlService {
    pub fn http(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    pub fn ws(&self, path: &str) -> String {
        format!("{}{path}", self.base_url.replacen("http://", "ws://", 1))
    }

    pub async fn shutdown(self) {
        self.engine_stop.notify_one();
        let _ = self.handle.await;
    }
}

fn base_config(channel: &str, pod: &str) -> EngineConfig {
    EngineConfig::new(
        ChannelName::new(channel).expect("a valid notify channel"),
        PodId::new(pod).expect("a valid pod id"),
    )
    .with_window(Duration::from_millis(30))
    .with_beat(Duration::from_millis(80))
    .with_lease(Duration::from_secs(5))
    .with_lock_timeout(Duration::from_millis(300))
}

async fn free_loopback_addr() -> std::net::SocketAddr {
    let probe = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port to discover a free address");
    probe.local_addr().expect("the probed address is known")
}

pub async fn boot_graphql_service(
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
    .expect("the graphql engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_projector(WidgetProjector)
        .expect("register the widget projector");
    engine
        .register_projector(AssignmentProjector)
        .expect("register the assignment projector");
    engine
        .register_mutation::<CloseWidget, _>(close_widget)
        .expect("register the close mutation");
    engine
        .register_mutation::<MintSecret, _>(mint_secret)
        .expect("register the mint mutation");
    engine
        .register_schema_slice(widget_slice())
        .expect("the widget slice owns its root fields and types");
    engine
        .register_schema_slice(assignment_slice())
        .expect("the assignment slice composes without colliding with the widget slice");

    let readiness = engine.readiness();
    let engine_stop = engine.shutdown_handle();
    let state = Arc::new(engine.graphql_state());
    let schema = engine_schema(
        QueryRoot::default(),
        MutationRoot,
        SubscriptionRoot,
        state.clone(),
    );
    engine.set_schema_sdl(schema.sdl());
    let app = service_engine::app(schema, state, readiness.clone());

    let handle = tokio::spawn(engine.run_with(app));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    while readiness.snapshot() != Readiness::Ready {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the graphql engine never reached readiness UP"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    GraphqlService {
        base_url: format!("http://{addr}"),
        engine_stop,
        handle,
    }
}

pub async fn boot_rls_query_service(
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
    .expect("the rls-query engine boots under the low-privilege app role");
    engine
        .register_principal_resolver(SamplePrincipalResolver)
        .expect("register the principal resolver");
    engine
        .register_rls(SampleRls)
        .expect("register the RLS applier so query-time fetch runs under RLS");
    engine
        .register_projector(RlsAssignmentProjector)
        .expect("register the RLS-backed assignment projector");
    engine
        .register_schema_slice(rls_assignment_slice())
        .expect("the rls-assignment slice owns its root field and type");

    let readiness = engine.readiness();
    let engine_stop = engine.shutdown_handle();
    let state = Arc::new(engine.graphql_state());
    let schema = engine_schema(
        RlsQueryRoot,
        async_graphql::EmptyMutation,
        async_graphql::EmptySubscription,
        state.clone(),
    );
    engine.set_schema_sdl(schema.sdl());
    let app = service_engine::app(schema, state, readiness.clone());

    let handle = tokio::spawn(engine.run_with(app));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    while readiness.snapshot() != Readiness::Ready {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the rls-query engine never reached readiness UP"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    GraphqlService {
        base_url: format!("http://{addr}"),
        engine_stop,
        handle,
    }
}

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
