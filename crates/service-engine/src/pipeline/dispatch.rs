use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use sqlx::{PgConnection, PgPool};

use crate::accumulator::AccumulatorRuntime;
use crate::error::EngineError;
use crate::inbound::Incoming;
use crate::inbound::sqlx_is_terminal;
use crate::inbound::{Applied, Dispatch, DispatchError, DispatchOutcome, NoOp};
use crate::inbound::{Claimed, Ordering, advance_sequence, claim};
use crate::inbound::{ReactionInvoker, ReactionRegistry};
use crate::pipeline::context::Reaction;
use crate::pipeline::ops::Ops;
use crate::pipeline::staged::Staged;
use crate::time;
use crate::transport::ImpactTransport;

pub(crate) struct DirectPipeline {
    pool: PgPool,
    transport: Arc<dyn ImpactTransport>,
    accumulators: Arc<AccumulatorRuntime>,
    reactions: Arc<ReactionRegistry>,
    lock_timeout: Duration,
    impacts_per_commit: usize,
}

impl DirectPipeline {
    pub(crate) fn new(
        pool: PgPool,
        transport: Arc<dyn ImpactTransport>,
        accumulators: Arc<AccumulatorRuntime>,
        reactions: Arc<ReactionRegistry>,
        lock_timeout: Duration,
        impacts_per_commit: usize,
    ) -> Self {
        Self {
            pool,
            transport,
            accumulators,
            reactions,
            lock_timeout,
            impacts_per_commit,
        }
    }

    async fn run(&self, msg: &Incoming) -> DispatchOutcome {
        let Some(invoker) = self.reactions.invoker(&msg.reaction).cloned() else {
            return DispatchOutcome::Failed(DispatchError::terminal(format!(
                "no reaction is registered under the durable {}",
                msg.reaction
            )));
        };
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(error) => return DispatchOutcome::Failed(classify(&error)),
        };
        if let Err(error) = set_lock_timeout(&mut tx, self.lock_timeout).await {
            let _ = tx.rollback().await;
            return DispatchOutcome::Failed(classify(&error));
        }
        match claim(&mut tx, msg.message_id, &msg.reaction).await {
            Ok(Claimed::Duplicate) => {
                let _ = tx.rollback().await;
                return DispatchOutcome::Applied(Applied::NoOp(NoOp::Duplicate));
            }
            Ok(Claimed::Fresh) => {}
            Err(error) => {
                let _ = tx.rollback().await;
                return DispatchOutcome::Failed(classify(&error));
            }
        }
        if let Some(sequence) = &msg.sequence {
            match advance_sequence(&mut tx, &sequence.key, sequence.seq).await {
                Ok(Ordering::Stale) => {
                    let _ = tx.rollback().await;
                    return DispatchOutcome::Applied(Applied::NoOp(NoOp::Stale));
                }
                Ok(Ordering::Advanced) => {}
                Err(error) => {
                    let _ = tx.rollback().await;
                    return DispatchOutcome::Failed(classify(&error));
                }
            }
        }
        let mut staged = Staged::default();
        let handler = {
            let ops = Ops::new(
                &mut tx,
                &mut staged,
                self.accumulators.as_ref(),
                time::now(),
            );
            let mut cx = Reaction::new(ops);
            invoke(invoker.as_ref(), &mut cx, &msg.payload).await
        };
        if let Err(error) = handler {
            let _ = tx.rollback().await;
            return DispatchOutcome::Failed(error);
        }
        if !staged.is_within(self.impacts_per_commit) {
            let _ = tx.rollback().await;
            return DispatchOutcome::Failed(DispatchError::terminal(format!(
                "the transaction dirtied {} keys, over impacts_per_commit={}; a change this \
                 large must go through the bulk path",
                staged.impacts.len(),
                self.impacts_per_commit
            )));
        }
        if let Err(error) = staged.flush(&mut tx, self.transport.as_ref()).await {
            let _ = tx.rollback().await;
            return DispatchOutcome::Failed(classify_engine(&error));
        }
        match tx.commit().await {
            Ok(()) => DispatchOutcome::Applied(Applied::Committed),
            Err(error) => DispatchOutcome::Failed(classify(&error)),
        }
    }
}

async fn invoke<'a>(
    invoker: &'a dyn ReactionInvoker,
    cx: &'a mut Reaction<'a>,
    payload: &'a [u8],
) -> Result<(), DispatchError> {
    invoker.invoke(cx, payload).await
}

impl Dispatch for DirectPipeline {
    fn dispatch<'a>(&'a self, msg: &'a Incoming) -> BoxFuture<'a, DispatchOutcome> {
        Box::pin(self.run(msg))
    }
}

pub(crate) async fn set_lock_timeout(
    conn: &mut PgConnection,
    lock_timeout: Duration,
) -> Result<(), sqlx::Error> {
    let millis = lock_timeout.as_millis().max(1);
    sqlx::query(&format!("SET LOCAL lock_timeout = {millis}"))
        .execute(conn)
        .await
        .map(|_| ())
}

pub(crate) fn classify(error: &sqlx::Error) -> DispatchError {
    if sqlx_is_terminal(error) {
        DispatchError::terminal(error.to_string())
    } else {
        DispatchError::retry(error.to_string())
    }
}

pub(crate) fn classify_engine(error: &EngineError) -> DispatchError {
    match error {
        EngineError::Db(db) => classify(db),
        EngineError::Encode { .. } | EngineError::Decode { .. } => {
            DispatchError::terminal(error.to_string())
        }
        _ => DispatchError::retry(error.to_string()),
    }
}
