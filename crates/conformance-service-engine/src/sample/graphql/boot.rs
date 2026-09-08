use std::sync::Arc;
use std::time::Duration;

use br_util_axum_readiness::{Readiness, ReadinessHandle};
use service_engine::config::EngineConfig;
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

pub struct GraphqlService {
    pub base_url: String,
    server_stop: Arc<Notify>,
    engine_stop: Arc<Notify>,
    server: JoinHandle<std::io::Result<()>>,
    engine: JoinHandle<Result<(), service_engine::EngineError>>,
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
        self.server_stop.notify_waiters();
        let _ = self.engine.await;
        let _ = self.server.await;
    }
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
    let server_stop = Arc::new(Notify::new());
    let server = tokio::spawn(service_engine::serve(listener, app, server_stop.clone()));
    let engine = tokio::spawn(engine.run());

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
        server_stop,
        engine_stop,
        server,
        engine,
    }
}
