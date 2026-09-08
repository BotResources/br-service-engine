mod error;
mod mutation;
mod principal;
mod query;
mod router;
mod schema;
mod state;
mod subscription;

pub use error::{CODE_EXTENSION, mutation_error};
pub use mutation::{MutationAck, ack, ack_bulk, execute, execute_bulk};
pub use principal::{AuthReject, PASSPORT_HEADER, PassportPrincipal, PrincipalRejected};
pub use query::{fetch, fetch_json, fetch_window, fetch_window_json};
pub use router::{app, serve};
pub use schema::engine_schema;
pub use state::GraphqlState;
pub use subscription::{
    EngineDelta, ProjectedView, RemovePayload, ResetPayload, UpsertPayload, attach, to_engine_delta,
};

pub use subscription::subscribe;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaKind {
    Reset,
    Upsert,
    Remove,
}
