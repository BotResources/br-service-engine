use std::sync::Arc;

use crate::readiness::{ReadinessHandle, readiness_route};
use async_graphql::http::ALL_WEBSOCKET_PROTOCOLS;
use async_graphql::{Data, ObjectType, Schema, SubscriptionType};
use async_graphql_axum::{GraphQLProtocol, GraphQLRequest, GraphQLResponse};
use axum::Router;
use axum::extract::{State, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use tokio::net::TcpListener;
use tokio::sync::Notify;

use crate::graphql::principal::{AuthReject, PASSPORT_HEADER, PassportPrincipal, resolve};
use crate::graphql::state::GraphqlState;

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

pub async fn serve(
    listener: TcpListener,
    app: Router,
    shutdown: Arc<Notify>,
) -> std::io::Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.notified().await })
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
        return unauthorized(AuthReject::Rejected(error.to_string()));
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
        return unauthorized(AuthReject::Rejected(error.to_string()));
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

fn unauthorized(reject: AuthReject) -> Response {
    (StatusCode::UNAUTHORIZED, reject.message()).into_response()
}
