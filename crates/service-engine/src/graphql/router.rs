use std::sync::Arc;

use crate::error::EngineError;
use crate::graphql::body::{self, BodyPolicy};
use crate::graphql::multipart::{MultipartPolicy, declares_upload_scalar};
use crate::graphql::principal::{AuthReject, PASSPORT_HEADER, PassportPrincipal, resolve};
use crate::graphql::refusal::coded_refusal;
use crate::graphql::sse;
use crate::graphql::state::GraphqlState;
use crate::graphql::{INTERNAL_CODE, INTERNAL_MESSAGE};
use crate::readiness::{ReadinessHandle, readiness_route};
use crate::stop::Stop;
use async_graphql::http::ALL_WEBSOCKET_PROTOCOLS;
use async_graphql::{Data, ObjectType, Schema, SubscriptionType};
use async_graphql_axum::{GraphQLProtocol, GraphQLResponse};
use axum::Router;
use axum::extract::{Request, State, WebSocketUpgrade};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, get, post};
use br_util_observability::{MetricsHandle, http_metrics_layer, liveness_route, metrics_route};
use tokio::net::TcpListener;

struct AppState<P: PassportPrincipal, Q, M, S> {
    schema: Schema<Q, M, S>,
    engine: Arc<GraphqlState<P>>,
    body: Arc<BodyPolicy>,
}

impl<P: PassportPrincipal, Q, M, S> Clone for AppState<P, Q, M, S> {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            engine: self.engine.clone(),
            body: self.body.clone(),
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
    let schema_declares_upload = declares_upload_scalar(&schema.sdl());
    let config = engine.runtime().config();
    let multipart = MultipartPolicy::new(&config.multipart, schema_declares_upload);
    tracing::debug!(
        schema_declares_upload,
        max_body_bytes = multipart.max_body_bytes(),
        max_files = multipart.max_files(),
        spool_dir = %multipart.spool_dir().display(),
        body_read_timeout_ms = %config.body_read_timeout.as_millis(),
        "graphql request body bounds"
    );
    let body = Arc::new(BodyPolicy::new(multipart, config.body_read_timeout));
    Router::new()
        .route("/graphql", post(graphql_post::<P, Q, M, S>))
        .route("/graphql/ws", get(graphql_ws::<P, Q, M, S>))
        .route("/readyz", readiness_route(readiness))
        .with_state(AppState {
            schema,
            engine,
            body,
        })
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
    request: Request,
) -> Response
where
    P: PassportPrincipal,
    Q: ObjectType + 'static,
    M: ObjectType + 'static,
    S: SubscriptionType + 'static,
{
    let (parts, body) = request.into_parts();
    // The principal is resolved from the headers alone, before a byte of the body is read:
    // a client without a trusted passport makes the pod neither buffer nor spool anything.
    let principal = match authenticate(&state.engine, &parts.headers).await {
        Ok(principal) => principal,
        Err(denied) => return denied.into_response(),
    };
    let request = match body::receive(&parts.headers, body, &state.body).await {
        Ok(request) => request.data(principal),
        Err(refusal) => return refusal.into_response(),
    };
    // The gateway's subscription leg: the same authenticated request, answered as a
    // graphql-sse stream bounded like a WebSocket session.
    if sse::wants_event_stream(&parts.headers) {
        return sse::respond(&state.schema, request, stream_bounds(&state.engine));
    }
    GraphQLResponse::from(state.schema.execute(request).await).into_response()
}

fn stream_bounds<P: PassportPrincipal>(engine: &GraphqlState<P>) -> sse::StreamBounds {
    sse::StreamBounds {
        max_age: engine.runtime().config().session_max_age,
        keep_alive: sse::KEEP_ALIVE_INTERVAL,
        shutdown: engine.stream_shutdown(),
    }
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
    let principal = match authenticate(&state.engine, &headers).await {
        Ok(principal) => principal,
        Err(denied) => return denied.into_response(),
    };
    let schema = state.schema.clone();
    let max_age = state.engine.runtime().config().session_max_age;
    let shutdown = state.engine.stream_shutdown();
    upgrade
        .protocols(ALL_WEBSOCKET_PROTOCOLS)
        .on_upgrade(move |socket| async move {
            let mut data = Data::default();
            data.insert(principal);
            crate::graphql::ws::serve_bounded(socket, schema, protocol, data, max_age, shutdown)
                .await
        })
}

/// Why a request gets no principal: a passport that is absent, undecodable or rejected
/// (`401`), or facts the pod could not load (`500` `INTERNAL`).
enum Denied {
    Unauthenticated(AuthReject),
    FactsUnavailable(Box<EngineError>),
}

impl IntoResponse for Denied {
    fn into_response(self) -> Response {
        match self {
            Self::Unauthenticated(reject) => unauthorized(reject),
            Self::FactsUnavailable(error) => facts_unavailable(&error),
        }
    }
}

/// The principal, from the headers alone: nothing of the body is read here.
async fn authenticate<P: PassportPrincipal>(
    engine: &GraphqlState<P>,
    headers: &HeaderMap,
) -> Result<P, Denied> {
    let mut principal = resolve::<P>(engine.pg(), passport_header(headers))
        .await
        .map_err(Denied::Unauthenticated)?;
    engine
        .runtime()
        .registry()
        .load_facts(engine.pg(), &mut principal)
        .await
        .map_err(|error| Denied::FactsUnavailable(Box::new(error)))?;
    Ok(principal)
}

fn passport_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(PASSPORT_HEADER)
        .and_then(|value| value.to_str().ok())
}

/// The principal's facts could not be loaded: an infrastructure fault, not a
/// rejected passport. The cause is logged with its chain; the client receives
/// `INTERNAL` in the GraphQL error shape and no detail.
fn facts_unavailable(error: &EngineError) -> Response {
    tracing::error!(
        cause = %crate::chain::describe(error),
        "loading the principal's facts failed; the client receives INTERNAL"
    );
    coded_refusal(
        StatusCode::INTERNAL_SERVER_ERROR,
        INTERNAL_CODE,
        INTERNAL_MESSAGE,
    )
}

fn unauthorized(reject: AuthReject) -> Response {
    (StatusCode::UNAUTHORIZED, reject.message()).into_response()
}

#[cfg(test)]
#[path = "router_tests.rs"]
mod tests;
