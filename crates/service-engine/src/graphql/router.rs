use std::sync::Arc;

use crate::graphql::principal::{AuthReject, PASSPORT_HEADER, PassportPrincipal, resolve};
use crate::graphql::state::GraphqlState;
use crate::readiness::{ReadinessHandle, readiness_route};
use crate::stop::Stop;
use async_graphql::http::ALL_WEBSOCKET_PROTOCOLS;
use async_graphql::{Data, ObjectType, Schema, SubscriptionType};
use async_graphql_axum::{GraphQLProtocol, GraphQLRequest, GraphQLResponse};
use axum::Router;
use axum::extract::{State, WebSocketUpgrade};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, get, post};
use br_util_observability::{MetricsHandle, http_metrics_layer, liveness_route, metrics_route};
use tokio::net::TcpListener;

struct AppState<P: PassportPrincipal, Q, M, S> {
    schema: Schema<Q, M, S>,
    engine: Arc<GraphqlState<P>>,
}

impl<P: PassportPrincipal, Q, M, S> Clone for AppState<P, Q, M, S> {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            engine: self.engine.clone(),
        }
    }
}

pub fn app<P, Q, M, S>(
    schema: Schema<Q, M, S>,
    engine: Arc<GraphqlState<P>>,
    readiness: ReadinessHandle,
) -> Router
where
    P: PassportPrincipal,
    Q: ObjectType + 'static,
    M: ObjectType + 'static,
    S: SubscriptionType + 'static,
{
    Router::new()
        .route("/graphql", post(graphql_post::<P, Q, M, S>))
        .route("/graphql/ws", get(graphql_ws::<P, Q, M, S>))
        .route("/readyz", readiness_route(readiness))
        .with_state(AppState { schema, engine })
}

const SDL_CONTENT_TYPE: &str = "text/plain; charset=utf-8";

pub(crate) fn with_edge_observability(app: Router, sdl: String, metrics: MetricsHandle) -> Router {
    app.route("/livez", liveness_route())
        .route("/metrics", metrics_route(metrics))
        .route("/sdl", sdl_route(sdl))
        .layer(http_metrics_layer())
}

pub fn with_sdl_route(app: Router, sdl: String) -> Router {
    app.route("/sdl", sdl_route(sdl))
}

fn sdl_route<S>(sdl: String) -> MethodRouter<S>
where
    S: Clone + Send + Sync + 'static,
{
    get(move || {
        let sdl = sdl.clone();
        async move {
            (
                StatusCode::OK,
                [(CONTENT_TYPE, HeaderValue::from_static(SDL_CONTENT_TYPE))],
                sdl,
            )
                .into_response()
        }
    })
}

pub async fn serve(listener: TcpListener, app: Router, shutdown: Arc<Stop>) -> std::io::Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.stopped().await })
        .await
}

async fn graphql_post<P, Q, M, S>(
    State(state): State<AppState<P, Q, M, S>>,
    headers: HeaderMap,
    request: GraphQLRequest,
) -> Response
where
    P: PassportPrincipal,
    Q: ObjectType + 'static,
    M: ObjectType + 'static,
    S: SubscriptionType + 'static,
{
    let mut principal = match resolve::<P>(state.engine.pg(), passport_header(&headers)).await {
        Ok(principal) => principal,
        Err(reject) => return unauthorized(reject),
    };
    if let Err(error) = state
        .engine
        .runtime()
        .registry()
        .load_facts(state.engine.pg(), &mut principal)
        .await
    {
        return facts_unavailable(&error);
    }
    let request = request.into_inner().data(principal);
    GraphQLResponse::from(state.schema.execute(request).await).into_response()
}

async fn graphql_ws<P, Q, M, S>(
    State(state): State<AppState<P, Q, M, S>>,
    protocol: GraphQLProtocol,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response
where
    P: PassportPrincipal,
    Q: ObjectType + 'static,
    M: ObjectType + 'static,
    S: SubscriptionType + 'static,
{
    let mut principal = match resolve::<P>(state.engine.pg(), passport_header(&headers)).await {
        Ok(principal) => principal,
        Err(reject) => return unauthorized(reject),
    };
    if let Err(error) = state
        .engine
        .runtime()
        .registry()
        .load_facts(state.engine.pg(), &mut principal)
        .await
    {
        return facts_unavailable(&error);
    }
    let schema = state.schema.clone();
    let max_age = state.engine.runtime().config().session_max_age;
    let shutdown = state.engine.ws_shutdown();
    upgrade
        .protocols(ALL_WEBSOCKET_PROTOCOLS)
        .on_upgrade(move |socket| async move {
            let mut data = Data::default();
            data.insert(principal);
            crate::graphql::ws::serve_bounded(socket, schema, protocol, data, max_age, shutdown)
                .await
        })
}

fn passport_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(PASSPORT_HEADER)
        .and_then(|value| value.to_str().ok())
}

/// The principal's facts could not be loaded: an infrastructure fault, not a
/// rejected passport. The cause is logged with its chain; the client receives
/// `INTERNAL` in the GraphQL error shape and no detail.
fn facts_unavailable(error: &crate::error::EngineError) -> Response {
    tracing::error!(
        cause = %crate::chain::describe(error),
        "loading the principal's facts failed; the client receives INTERNAL"
    );
    let body = serde_json::json!({
        "errors": [{
            "message": crate::graphql::INTERNAL_MESSAGE,
            "extensions": { crate::graphql::CODE_EXTENSION: crate::graphql::INTERNAL_CODE },
        }],
    });
    (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(body)).into_response()
}

fn unauthorized(reject: AuthReject) -> Response {
    (StatusCode::UNAUTHORIZED, reject.message()).into_response()
}
