use std::sync::{Arc, RwLock};

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, get};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Readiness {
    Ready,
    NotReady { reason: String },
}

impl Readiness {
    pub fn is_ready(&self) -> bool {
        matches!(self, Readiness::Ready)
    }
}

#[derive(Clone)]
pub struct ReadinessHandle {
    state: Arc<RwLock<Readiness>>,
}

impl ReadinessHandle {
    pub fn ready() -> Self {
        Self {
            state: Arc::new(RwLock::new(Readiness::Ready)),
        }
    }

    pub fn not_ready(reason: impl Into<String>) -> Self {
        Self {
            state: Arc::new(RwLock::new(Readiness::NotReady {
                reason: reason.into(),
            })),
        }
    }

    pub fn set_ready(&self) {
        let was_not_ready = {
            let mut guard = self.state.write().unwrap_or_else(|e| e.into_inner());
            let was_not_ready = !guard.is_ready();
            *guard = Readiness::Ready;
            was_not_ready
        };
        if was_not_ready {
            tracing::info!("readiness: UP");
        }
    }

    pub fn set_not_ready(&self, reason: impl Into<String>) {
        let reason = reason.into();
        let changed = {
            let mut guard = self.state.write().unwrap_or_else(|e| e.into_inner());
            let changed = match &*guard {
                Readiness::Ready => true,
                Readiness::NotReady { reason: prev } => prev != &reason,
            };
            *guard = Readiness::NotReady {
                reason: reason.clone(),
            };
            changed
        };
        if changed {
            tracing::warn!(reason = %reason, "readiness: DOWN");
        }
    }

    pub fn snapshot(&self) -> Readiness {
        self.state.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn is_ready(&self) -> bool {
        self.snapshot().is_ready()
    }
}

pub fn readiness_route<S>(handle: ReadinessHandle) -> MethodRouter<S>
where
    S: Clone + Send + Sync + 'static,
{
    get(move || {
        let handle = handle.clone();
        async move {
            match handle.snapshot() {
                Readiness::Ready => (StatusCode::OK, "ready").into_response(),
                Readiness::NotReady { reason } => {
                    (StatusCode::SERVICE_UNAVAILABLE, reason).into_response()
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_share_one_state_and_a_flip_is_visible_across_them() {
        let handle = ReadinessHandle::not_ready("starting up");
        let twin = handle.clone();
        handle.set_ready();
        assert!(twin.is_ready());
        twin.set_not_ready("dependency unavailable");
        assert_eq!(
            handle.snapshot(),
            Readiness::NotReady {
                reason: "dependency unavailable".to_string()
            }
        );
    }

    #[test]
    fn a_poisoned_lock_still_answers_rather_than_panicking() {
        let handle = ReadinessHandle::ready();
        let poison_target = handle.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poison_target.state.write().unwrap();
            panic!("poison the readiness lock mid-mutation");
        })
        .join();
        assert!(handle.state.is_poisoned());
        assert!(handle.is_ready());
        handle.set_not_ready("down");
        assert!(!handle.is_ready());
    }
}
