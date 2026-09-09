use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use sqlx::{PgConnection, PgPool};

use crate::accumulator::AccumulatorRuntime;
use crate::blobs::BlobHandle;
use crate::error::EngineError;
use crate::inbound::Incoming;
use crate::inbound::sqlx_is_terminal;
use crate::inbound::{Applied, Dispatch, DispatchError, DispatchOutcome, NoOp};
use crate::inbound::{Claimed, Ordering, advance_sequence, claim};
use crate::inbound::{ReactionInvoker, ReactionRegistry};
use crate::offers::OfferStagers;
use crate::pipeline::context::Reaction;
use crate::pipeline::ops::Ops;
use crate::pipeline::staged::Staged;
use crate::time;
use crate::transport::ImpactTransport;

pub(crate) struct DirectPipeline {
    pool: PgPool,
    transport: Arc<dyn ImpactTransport>,
    accumulators: Arc<AccumulatorRuntime>,
    offers: Arc<OfferStagers>,
    reactions: Arc<ReactionRegistry>,
    blobs: Option<BlobHandle>,
    lock_timeout: Duration,
    impacts_per_commit: usize,
}

impl DirectPipeline {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        pool: PgPool,
        transport: Arc<dyn ImpactTransport>,
        accumulators: Arc<AccumulatorRuntime>,
        offers: Arc<OfferStagers>,
        reactions: Arc<ReactionRegistry>,
        blobs: Option<BlobHandle>,
        lock_timeout: Duration,
        impacts_per_commit: usize,
    ) -> Self {
        Self {
            pool,
            transport,
            accumulators,
            offers,
            reactions,
            blobs,
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
        let mut tx = match begin_scoped(&self.pool, self.lock_timeout).await {
            Ok(tx) => tx,
            Err(error) => return DispatchOutcome::Failed(classify(&error)),
        };
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
                self.offers.clone(),
                self.blobs.as_ref(),
                time::now(),
            );
            let mut cx = Reaction::new(ops, msg.delivered);
            invoke(invoker.as_ref(), &mut cx, &msg.payload).await
        };
        if let Err(error) = handler {
            let _ = tx.rollback().await;
            let error = match staged.terminal_violation.take() {
                Some(detail) => DispatchError::terminal(detail),
                None => error,
            };
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
        match flush_and_commit(tx, &staged, self.transport.as_ref()).await {
            Ok(()) => {
                self.accumulators
                    .purge_committed_seals(&staged.sealed_keys)
                    .await;
                DispatchOutcome::Applied(Applied::Committed)
            }
            Err(error) => DispatchOutcome::Failed(classify_engine(&error)),
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

pub(crate) async fn begin_scoped(
    pool: &PgPool,
    lock_timeout: Duration,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    if let Err(error) = set_lock_timeout(&mut tx, lock_timeout).await {
        let _ = tx.rollback().await;
        return Err(error);
    }
    Ok(tx)
}

pub(crate) async fn flush_and_commit(
    mut tx: sqlx::Transaction<'static, sqlx::Postgres>,
    staged: &Staged,
    transport: &dyn ImpactTransport,
) -> Result<(), EngineError> {
    if let Err(error) = staged.flush(&mut tx, transport).await {
        let _ = tx.rollback().await;
        return Err(error);
    }
    tx.commit().await.map_err(EngineError::from)?;
    crate::observe::record_impacts_committed(staged.impacts.len());
    Ok(())
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
