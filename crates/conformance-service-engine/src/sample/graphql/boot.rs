use std::sync::Arc;
use std::time::Duration;

use br_util_axum_readiness::{Readiness, ReadinessHandle};
use service_engine::config::EngineConfig;
use service_engine::graphql::SliceFragment;
use service_engine::name::{ChannelName, PodId};
use service_engine::nats::Nats;
use service_engine::{Engine, engine_schema};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::infra::TestDb;
use crate::sample::assignment::AssignmentProjector;
use crate::sample::graphql::roots::{MutationRoot, QueryRoot, SubscriptionRoot};
use crate::sample::pipeline::{CloseWidget, MintSecret, close_widget, mint_secret};
use crate::sample::principal::{SamplePrincipal, SamplePrincipalResolver};
use crate::sample::widget::WidgetProjector;

const WIDGET_SLICE: SliceFragment = SliceFragment {
    slice: "widget",
    root_fields: &["widget", "closeWidget", "mintSecret", "widgets"],
    types: &["WidgetView"],
};

const ASSIGNMENT_SLICE: SliceFragment = SliceFragment {
    slice: "assignment",
    root_fields: &["assignment", "assignments"],
    types: &["AssignmentView"],
};

pub const ROOT_FIELD_COLLISION: SliceFragment = SliceFragment {
    slice: "shadow",
    root_fields: &["widget"],
    types: &["ShadowView"],
};

pub const TYPE_COLLISION: SliceFragment = SliceFragment {
    slice: "shadow",
    root_fields: &["shadow"],
    types: &["WidgetView"],
};

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
    let config = EngineConfig::new(
        ChannelName::new(channel).expect("a valid notify channel"),
        PodId::new(pod).expect("a valid pod id"),
    )
    .with_window(Duration::from_millis(30))
    .with_beat(Duration::from_millis(80))
    .with_lease(Duration::from_secs(5))
    .with_lock_timeout(Duration::from_millis(300))
    .with_http_addr(addr);

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
        .register_schema_slice(WIDGET_SLICE)
        .expect("the widget slice owns its root fields and types");
    engine
        .register_schema_slice(ASSIGNMENT_SLICE)
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

pub async fn boot_colliding_slices(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
    colliding: SliceFragment,
) -> Result<(), service_engine::EngineError> {
    let config = EngineConfig::new(
        ChannelName::new(channel).expect("a valid notify channel"),
        PodId::new(pod).expect("a valid pod id"),
    )
    .with_window(Duration::from_millis(30))
    .with_beat(Duration::from_millis(80))
    .with_lease(Duration::from_secs(5))
    .with_lock_timeout(Duration::from_millis(300));

    let mut engine = Engine::<SamplePrincipal>::boot(
        config,
        db.app_pool().clone(),
        nats,
        ReadinessHandle::ready(),
    )
    .await?;
    engine.register_principal_resolver(SamplePrincipalResolver)?;
    engine.register_projector(WidgetProjector)?;
    engine.register_schema_slice(WIDGET_SLICE)?;
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
