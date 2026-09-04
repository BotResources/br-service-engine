use std::time::Duration;

mod previous;

use crate::cron::{CronExpr, NextFire, Schedule};
use crate::error::CronError;
use crate::time::Timestamp;

pub(super) fn at(rfc3339: &str) -> Timestamp {
    rfc3339.parse().unwrap()
}

pub(super) const WEEK: Duration = Duration::from_secs(7 * 24 * 60 * 60);
pub(super) const BEAT: Duration = Duration::from_secs(1);

#[test]
fn an_interval_schedule_fires_on_its_anchor_when_nothing_has_elapsed_yet() {
    let anchor = at("2026-01-01T00:00:00Z");
    let schedule = Schedule::Every {
        period: 3 * WEEK,
        anchor,
    };
    assert_eq!(
        schedule
            .next_fire(at("2025-12-31T23:59:59Z"), BEAT)
            .unwrap(),
        NextFire::At(anchor)
    );
}

#[test]
fn an_interval_schedule_fires_at_the_anchor_plus_a_whole_number_of_periods() {
    let schedule = Schedule::Every {
        period: 3 * WEEK,
        anchor: at("2026-01-01T00:00:00Z"),
    };
    for (after, expected) in [
        ("2026-01-01T00:00:00Z", "2026-01-22T00:00:00Z"),
        ("2026-01-21T23:59:59Z", "2026-01-22T00:00:00Z"),
        ("2026-01-22T00:00:00Z", "2026-02-12T00:00:00Z"),
        ("2026-06-30T12:00:00Z", "2026-07-09T00:00:00Z"),
    ] {
        assert_eq!(
            schedule.next_fire(at(after), BEAT).unwrap(),
            NextFire::At(at(expected)),
            "after {after}"
        );
    }
}

#[test]
fn an_interval_schedule_never_returns_a_fire_time_that_is_not_strictly_after_the_caller() {
    let anchor = at("2026-01-01T00:00:00Z");
    let schedule = Schedule::Every {
        period: Duration::from_millis(250),
        anchor,
    };
    for offset_ms in [0, 1, 249, 250, 251, 1_000] {
        let after = anchor + chrono::TimeDelta::milliseconds(offset_ms);
        let NextFire::At(fire) = schedule.next_fire(after, BEAT).unwrap() else {
            panic!("an interval schedule must fire at a time");
        };
        assert!(fire > after, "offset {offset_ms}ms yielded {fire}");
        assert!(
            (fire - anchor).num_milliseconds() % 250 == 0,
            "offset {offset_ms}ms left the lattice at {fire}"
        );
    }
}

#[test]
fn an_interval_schedule_with_no_period_is_refused_instead_of_spinning() {
    let schedule = Schedule::Every {
        period: Duration::ZERO,
        anchor: at("2026-01-01T00:00:00Z"),
    };
    assert!(matches!(
        schedule.next_fire(at("2026-01-01T00:00:00Z"), BEAT),
        Err(CronError::Schedule { .. })
    ));
}

#[test]
fn a_period_finer_than_a_slot_is_refused_instead_of_collapsing_two_slots_into_one() {
    for period in [Duration::from_nanos(1), Duration::from_nanos(999)] {
        let schedule = Schedule::Every {
            period,
            anchor: at("2026-01-01T00:00:00Z"),
        };
        assert!(
            matches!(
                schedule.next_fire(at("2026-01-01T00:00:00Z"), BEAT),
                Err(CronError::Schedule { .. })
            ),
            "{period:?} was accepted"
        );
    }
    let schedule = Schedule::Every {
        period: Duration::from_micros(1),
        anchor: at("2026-01-01T00:00:00Z"),
    };
    assert!(schedule.next_fire(at("2026-01-01T00:00:00Z"), BEAT).is_ok());
}

#[test]
fn an_interval_schedule_past_the_end_of_time_never_fires_again() {
    let schedule = Schedule::Every {
        period: Duration::from_secs(u64::from(u32::MAX)) * 1_000_000,
        anchor: at("2026-01-01T00:00:00Z"),
    };
    assert_eq!(
        schedule
            .next_fire(at("2026-01-01T00:00:00Z"), BEAT)
            .unwrap(),
        NextFire::Never
    );
}

#[test]
fn a_beat_counted_schedule_fires_on_a_wall_clock_lattice_anchored_on_the_unix_epoch() {
    let schedule = Schedule::EveryBeats(4);
    for (after, expected) in [
        ("2026-01-01T00:00:00Z", "2026-01-01T00:00:04Z"),
        ("2026-01-01T00:00:01Z", "2026-01-01T00:00:04Z"),
        ("2026-01-01T00:00:03Z", "2026-01-01T00:00:04Z"),
        ("2026-01-01T00:00:04Z", "2026-01-01T00:00:08Z"),
    ] {
        assert_eq!(
            schedule.next_fire(at(after), BEAT).unwrap(),
            NextFire::At(at(expected)),
            "after {after}"
        );
    }
}

#[test]
fn a_beat_counted_schedule_gives_two_pods_with_skewed_clocks_the_same_slot() {
    let schedule = Schedule::EveryBeats(4);
    let one = at("2026-01-01T00:00:01Z") + chrono::TimeDelta::nanoseconds(7);
    let other = at("2026-01-01T00:00:03Z") + chrono::TimeDelta::nanoseconds(999_999_999);
    assert_eq!(
        schedule.next_fire(one, BEAT).unwrap(),
        schedule.next_fire(other, BEAT).unwrap()
    );
}

#[test]
fn a_beat_counted_schedule_refuses_a_count_of_zero_and_a_beat_of_zero() {
    let after = at("2026-01-01T00:00:00Z");
    assert!(matches!(
        Schedule::EveryBeats(0).next_fire(after, BEAT),
        Err(CronError::Schedule { .. })
    ));
    assert!(matches!(
        Schedule::EveryBeats(4).next_fire(after, Duration::ZERO),
        Err(CronError::Schedule { .. })
    ));
}

#[test]
fn an_expression_that_matches_no_real_date_never_fires_instead_of_erroring_forever() {
    let expr = CronExpr::new("0 0 30 2 *").unwrap();
    assert_eq!(
        expr.next_fire(at("2026-01-01T00:00:00Z")).unwrap(),
        NextFire::Never
    );
}

#[test]
fn a_fire_time_is_the_same_slot_however_many_sub_second_nanos_the_caller_carries() {
    let expr = CronExpr::new("*/15 * * * *").unwrap();
    let base = at("2026-01-01T00:07:00Z");
    let expected = NextFire::At(at("2026-01-01T00:15:00Z"));
    for nanos in [0, 1, 500_000_000, 999_999_999] {
        let after = base + chrono::TimeDelta::nanoseconds(nanos);
        assert_eq!(expr.next_fire(after).unwrap(), expected, "{nanos}ns");
    }
}

#[test]
fn an_interval_fire_time_is_the_same_slot_however_many_sub_second_nanos_the_caller_carries() {
    let anchor = at("2026-01-01T00:00:00Z");
    let schedule = Schedule::Every {
        period: Duration::from_secs(15 * 60),
        anchor,
    };
    let base = at("2026-01-01T00:07:00Z");
    let expected = NextFire::At(at("2026-01-01T00:15:00Z"));
    for nanos in [0, 1, 500_000_000, 999_999_999] {
        let after = base + chrono::TimeDelta::nanoseconds(nanos);
        assert_eq!(
            schedule.next_fire(after, BEAT).unwrap(),
            expected,
            "{nanos}ns"
        );
    }
}

#[test]
fn a_cron_fire_time_lands_on_a_whole_second() {
    let expr = CronExpr::new("0 0 * * *").unwrap();
    let NextFire::At(fire) = expr
        .next_fire(at("2026-01-01T09:13:07Z") + chrono::TimeDelta::nanoseconds(123_456_789))
        .unwrap()
    else {
        panic!("a daily expression must fire at a time");
    };
    assert_eq!(fire, at("2026-01-02T00:00:00Z"));
}

#[test]
fn an_interval_fire_time_keeps_the_sub_second_offset_of_its_anchor() {
    let anchor = at("2026-01-01T00:00:00Z") + chrono::TimeDelta::milliseconds(250);
    let schedule = Schedule::Every {
        period: Duration::from_secs(60),
        anchor,
    };
    assert_eq!(
        schedule
            .next_fire(at("2026-01-01T00:00:30Z"), BEAT)
            .unwrap(),
        NextFire::At(anchor + chrono::TimeDelta::seconds(60))
    );
}

#[test]
fn a_schedule_carrying_an_expression_delegates_to_it() {
    let schedule = Schedule::Cron(CronExpr::new("0 3 L * *").unwrap());
    assert_eq!(
        schedule
            .next_fire(at("2026-01-15T00:00:00Z"), BEAT)
            .unwrap(),
        NextFire::At(at("2026-01-31T03:00:00Z"))
    );
}
