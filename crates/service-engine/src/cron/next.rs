use std::time::Duration;

use chrono::TimeDelta;
use croner::Cron;
use croner::errors::CronError as CronerError;

use crate::error::CronError;
use crate::time::Timestamp;

use super::Schedule;

const NANOS_PER_SECOND: i128 = 1_000_000_000;
const SLOT_RESOLUTION_NANOS: i128 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NextFire {
    At(Timestamp),
    Never,
}

pub(super) fn next_fire(
    schedule: &Schedule,
    after: Timestamp,
    beat: Duration,
) -> Result<NextFire, CronError> {
    match schedule {
        Schedule::EveryBeats(beats) => {
            every_next_fire(beat_period(*beats, beat)?, Timestamp::UNIX_EPOCH, after)
        }
        Schedule::Cron(expr) => expr.next_fire(after),
        Schedule::Every { period, anchor } => every_next_fire(*period, *anchor, after),
    }
}

pub(super) fn previous_fire(
    schedule: &Schedule,
    before: Timestamp,
    beat: Duration,
) -> Result<NextFire, CronError> {
    match schedule {
        Schedule::EveryBeats(beats) => {
            every_previous_fire(beat_period(*beats, beat)?, Timestamp::UNIX_EPOCH, before)
        }
        Schedule::Cron(expr) => expr.previous_fire(before),
        Schedule::Every { period, anchor } => every_previous_fire(*period, *anchor, before),
    }
}

pub(super) fn cron_next_fire(
    cron: &Cron,
    source: &str,
    after: Timestamp,
) -> Result<NextFire, CronError> {
    match cron.find_next_occurrence(&after.as_datetime(), false) {
        Ok(at) => Ok(NextFire::At(Timestamp::from_utc(at))),
        Err(CronerError::TimeSearchLimitExceeded) => Ok(NextFire::Never),
        Err(reason) => Err(CronError::Expr {
            expr: source.to_string(),
            reason: reason.to_string(),
        }),
    }
}

pub(super) fn cron_previous_fire(
    cron: &Cron,
    source: &str,
    before: Timestamp,
) -> Result<NextFire, CronError> {
    match cron.find_previous_occurrence(&before.as_datetime(), true) {
        Ok(at) => Ok(NextFire::At(Timestamp::from_utc(at))),
        Err(CronerError::TimeSearchLimitExceeded) => Ok(NextFire::Never),
        Err(reason) => Err(CronError::Expr {
            expr: source.to_string(),
            reason: reason.to_string(),
        }),
    }
}

fn every_previous_fire(
    period: Duration,
    anchor: Timestamp,
    before: Timestamp,
) -> Result<NextFire, CronError> {
    let period = period_nanos(period)?;
    if before < anchor {
        return Ok(NextFire::Never);
    }
    let elapsed = elapsed_nanos(before - anchor);
    let Some(offset) = (elapsed / period).checked_mul(period).and_then(time_delta) else {
        return Ok(NextFire::Never);
    };
    match anchor.checked_add_signed(offset) {
        Some(at) => Ok(NextFire::At(at)),
        None => Ok(NextFire::Never),
    }
}

fn beat_period(beats: u32, beat: Duration) -> Result<Duration, CronError> {
    if beats == 0 {
        return Err(CronError::Schedule {
            reason: "EveryBeats(0) has no period".to_string(),
        });
    }
    beat.checked_mul(beats).ok_or_else(|| CronError::Schedule {
        reason: format!("{beats} beats of {beat:?} is out of range"),
    })
}

fn elapsed_nanos(delta: TimeDelta) -> i128 {
    i128::from(delta.num_seconds()) * NANOS_PER_SECOND + i128::from(delta.subsec_nanos())
}

fn every_next_fire(
    period: Duration,
    anchor: Timestamp,
    after: Timestamp,
) -> Result<NextFire, CronError> {
    let period = period_nanos(period)?;
    if after < anchor {
        return Ok(NextFire::At(anchor));
    }
    let elapsed = elapsed_nanos(after - anchor);
    let Some(offset) = (elapsed / period + 1)
        .checked_mul(period)
        .and_then(time_delta)
    else {
        return Ok(NextFire::Never);
    };
    match anchor.checked_add_signed(offset) {
        Some(at) => Ok(NextFire::At(at)),
        None => Ok(NextFire::Never),
    }
}

fn period_nanos(period: Duration) -> Result<i128, CronError> {
    let nanos = i128::try_from(period.as_nanos()).map_err(|_| CronError::Schedule {
        reason: format!("the period {period:?} is out of range"),
    })?;
    if nanos < SLOT_RESOLUTION_NANOS {
        return Err(CronError::Schedule {
            reason: format!(
                "the period {period:?} is shorter than the one microsecond a slot can hold"
            ),
        });
    }
    Ok(nanos)
}

fn time_delta(nanos: i128) -> Option<TimeDelta> {
    let seconds = i64::try_from(nanos / NANOS_PER_SECOND).ok()?;
    let rest = i64::try_from(nanos % NANOS_PER_SECOND).ok()?;
    TimeDelta::try_seconds(seconds)?.checked_add(&TimeDelta::nanoseconds(rest))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod vectors;
