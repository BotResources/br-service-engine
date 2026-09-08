use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool};

use crate::inbound::dispatch::{Applied, Dispatch, DispatchError, DispatchOutcome, NoOp};
use crate::inbound::disposition::sqlx_is_terminal;
use crate::inbound::guard::{Claimed, Ordering, advance_sequence, claim};
use crate::inbound::message::Incoming;
use crate::schema::TABLE_MESSAGE_CLAIM;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StubVerdict {
    Commit,
    Retry,
    Park,
    Terminal,
    PgViolation,
    ParkUntilReleased,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StubPayload {
    pub verdict: StubVerdict,
    #[serde(default)]
    pub note: Option<String>,
}

impl StubPayload {
    pub fn commit() -> Self {
        Self {
            verdict: StubVerdict::Commit,
            note: None,
        }
    }

    pub fn of(verdict: StubVerdict) -> Self {
        Self {
            verdict,
            note: None,
        }
    }
}

pub type EffectWriter = dyn for<'c> Fn(&'c mut PgConnection, &'c Incoming) -> BoxFuture<'c, Result<(), sqlx::Error>>
    + Send
    + Sync;

#[derive(Clone)]
pub struct StubDispatch {
    pool: PgPool,
    crash_remaining: Arc<AtomicUsize>,
    release: Arc<AtomicBool>,
    effect: Option<Arc<EffectWriter>>,
}

impl StubDispatch {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            crash_remaining: Arc::new(AtomicUsize::new(0)),
            release: Arc::new(AtomicBool::new(false)),
            effect: None,
        }
    }

    pub fn with_effect(mut self, effect: Arc<EffectWriter>) -> Self {
        self.effect = Some(effect);
        self
    }

    pub fn crash_before_commit(self, times: usize) -> Self {
        self.crash_remaining.store(times, AtomicOrdering::SeqCst);
        self
    }

    pub fn release_gate(&self) -> Arc<AtomicBool> {
        self.release.clone()
    }

    async fn apply(&self, msg: &Incoming) -> DispatchOutcome {
        let payload: StubPayload = match serde_json::from_slice(&msg.payload) {
            Ok(payload) => payload,
            Err(error) => {
                return DispatchOutcome::Failed(DispatchError::terminal(error.to_string()));
            }
        };
        match payload.verdict {
            StubVerdict::Retry => {
                return DispatchOutcome::Failed(DispatchError::retry("stub retry"));
            }
            StubVerdict::Terminal => {
                return DispatchOutcome::Failed(DispatchError::terminal("stub terminal"));
            }
            StubVerdict::Park => return DispatchOutcome::Failed(DispatchError::park("stub park")),
            StubVerdict::ParkUntilReleased if !self.release.load(AtomicOrdering::SeqCst) => {
                return DispatchOutcome::Failed(DispatchError::park("waiting for the aggregate"));
            }
            _ => {}
        }
        self.transact(msg, payload.verdict).await
    }

    async fn transact(&self, msg: &Incoming, verdict: StubVerdict) -> DispatchOutcome {
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(error) => return DispatchOutcome::Failed(DispatchError::retry(error.to_string())),
        };
        match claim(&mut tx, msg.message_id, &msg.reaction).await {
            Ok(Claimed::Duplicate) => {
                let _ = tx.rollback().await;
                return DispatchOutcome::Applied(Applied::NoOp(NoOp::Duplicate));
            }
            Ok(Claimed::Fresh) => {}
            Err(error) => return DispatchOutcome::Failed(classify(error)),
        }
        if let Some(sequence) = &msg.sequence {
            match advance_sequence(&mut tx, &sequence.key, sequence.seq).await {
                Ok(Ordering::Stale) => {
                    let _ = tx.rollback().await;
                    return DispatchOutcome::Applied(Applied::NoOp(NoOp::Stale));
                }
                Ok(Ordering::Advanced) => {}
                Err(error) => return DispatchOutcome::Failed(classify(error)),
            }
        }
        if verdict == StubVerdict::PgViolation
            && let Some(outcome) = self.provoke_violation(&mut tx, msg).await
        {
            return outcome;
        }
        if let Some(effect) = &self.effect
            && let Err(error) = effect(&mut tx, msg).await
        {
            return DispatchOutcome::Failed(classify(error));
        }
        if self
            .crash_remaining
            .fetch_update(AtomicOrdering::SeqCst, AtomicOrdering::SeqCst, |n| {
                (n > 0).then(|| n - 1)
            })
            .is_ok()
        {
            drop(tx);
            return DispatchOutcome::Failed(DispatchError::retry("simulated crash before commit"));
        }
        match tx.commit().await {
            Ok(()) => DispatchOutcome::Applied(Applied::Committed),
            Err(error) => DispatchOutcome::Failed(classify(error)),
        }
    }

    async fn provoke_violation(
        &self,
        tx: &mut PgConnection,
        msg: &Incoming,
    ) -> Option<DispatchOutcome> {
        let sql =
            format!("INSERT INTO {TABLE_MESSAGE_CLAIM} (message_id, reaction) VALUES ($1, $2)");
        let attempt = sqlx::query(&sql)
            .bind(msg.message_id)
            .bind(&msg.reaction)
            .execute(&mut *tx)
            .await;
        match attempt {
            Ok(_) => None,
            Err(error) if sqlx_is_terminal(&error) => {
                Some(DispatchOutcome::Failed(DispatchError::terminal(format!(
                    "integrity violation classified terminal: {error}"
                ))))
            }
            Err(error) => Some(DispatchOutcome::Failed(DispatchError::retry(
                error.to_string(),
            ))),
        }
    }
}

fn classify(error: sqlx::Error) -> DispatchError {
    if sqlx_is_terminal(&error) {
        DispatchError::terminal(error.to_string())
    } else {
        DispatchError::retry(error.to_string())
    }
}

impl Dispatch for StubDispatch {
    fn dispatch<'a>(&'a self, msg: &'a Incoming) -> BoxFuture<'a, DispatchOutcome> {
        Box::pin(self.apply(msg))
    }
}
