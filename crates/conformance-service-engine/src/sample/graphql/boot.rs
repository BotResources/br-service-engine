use std::sync::Arc;
use std::time::Duration;

use br_util_axum_readiness::{Readiness, ReadinessHandle};
use service_engine::config::EngineConfig;
use service_engine::graphql::{SchemaSlices, SliceFragment};
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

fn assemble_slices() -> SchemaSlices {
    let mut slices = SchemaSlices::new();
    slices
        .add(WIDGET_SLICE)
        .expect("the widget slice owns its root fields and types");
    slices
        .add(ASSIGNMENT_SLICE)
        .expect("the assignment slice composes without colliding with the widget slice");
    slices
}

pub async fn boot_graphql_service(
    db: &TestDb,
    nats: Nats,
    channel: &str,
    pod: &str,
) -> GraphqlService {
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

    let _slices = assemble_slices();

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

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port for the graphql server");
    let addr = listener.local_addr().expect("the bound address is known");
    let handle = tokio::spawn(engine.run_with_listener(listener, app));

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
