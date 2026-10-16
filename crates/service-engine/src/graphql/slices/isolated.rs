use std::sync::Arc;
use std::thread;

use tokio::sync::oneshot;

use super::SchemaSlices;
use crate::error::{CompositionError, EngineError};

const VERIFY_THREAD_NAME: &str = "schema-verify";
const VERIFY_STACK_BYTES: usize = 8 * 1024 * 1024;

impl SchemaSlices {
    pub(crate) async fn verify_on_known_stack(
        self: &Arc<Self>,
        sdl: String,
    ) -> Result<(), EngineError> {
        let slices = Arc::clone(self);
        run_on_known_stack(move || slices.verify(&sdl)).await?
    }
}

pub(super) async fn run_on_known_stack<T, F>(work: F) -> Result<T, EngineError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (verdict_tx, verdict_rx) = oneshot::channel();
    thread::Builder::new()
        .name(VERIFY_THREAD_NAME.to_string())
        .stack_size(VERIFY_STACK_BYTES)
        .spawn(move || {
            let _ = verdict_tx.send(work());
        })
        .map_err(|source| {
            refused(format!(
                "the verification thread could not start: {}",
                crate::chain::describe(&source)
            ))
        })?;
    verdict_rx.await.map_err(|_| {
        refused("the verification thread ended without a verdict; it panicked".to_string())
    })
}

fn refused(detail: String) -> EngineError {
    EngineError::Composition(CompositionError::SchemaParse { detail })
}
