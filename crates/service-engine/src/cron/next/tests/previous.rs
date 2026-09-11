use std::time::Duration;

use crate::cron::{CronExpr, NextFire, Schedule};
use crate::error::CronError;

use super::{BEAT, WEEK, at};

#[test]
fn an_interval_schedule_names_the_newest_slot_that_already_fired() {
    let anchor = at("2026-01-01T00:00:00Z");
    let schedule = Schedule::Every {
        period: 3 * WEEK,
        anchor,
    };
    for (before, expected) in [
        ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        ("2026-01-21T23:59:59Z", "2026-01-01T00:00:00Z"),
        ("2026-01-22T00:00:00Z", "2026-01-22T00:00:00Z"),
        ("2026-06-30T12:00:00Z", "2026-06-18T00:00:00Z"),
    ] {
        assert_eq!(
            schedule.previous_fire(at(before), BEAT).unwrap(),
            NextFire::At(at(expected)),
            "before {before}"
        );
    }
}

#[test]
fn a_schedule_that_has_never_fired_yet_has_no_previous_slot_to_catch_up_on() {
    let anchor = at("2026-01-01T00:00:00Z");
    let schedule = Schedule::Every {
        period: 3 * WEEK,
        anchor,
    };
    assert_eq!(
        schedule.previous_fire(anchor, BEAT).unwrap(),
        NextFire::At(anchor),
        "the anchor is itself the first due slot"
    );
    assert_eq!(
        schedule
            .previous_fire(at("2025-12-31T23:59:59Z"), BEAT)
            .unwrap(),
        NextFire::Never,
        "before the anchor there is no slot to catch up on"
    );
}

#[test]
fn the_due_slot_is_the_newest_one_at_or_before_now_and_the_next_one_is_strictly_after() {
    let schedule = Schedule::EveryBeats(4);
    let now = at("2026-01-01T00:00:08Z");
    assert_eq!(
        schedule.previous_fire(now, BEAT).unwrap(),
        NextFire::At(now),
        "a slot whose time has exactly arrived is due, not skipped"
    );
    assert_eq!(
        schedule
            .previous_fire(now - chrono::TimeDelta::nanoseconds(1), BEAT)
            .unwrap(),
        NextFire::At(at("2026-01-01T00:00:04Z"))
    );
    assert_eq!(
        schedule.next_fire(now, BEAT).unwrap(),
        NextFire::At(at("2026-01-01T00:00:12Z"))
    );
}

#[test]
fn a_beat_counted_previous_slot_is_the_same_wall_clock_lattice_two_pods_agree_on() {
    let schedule = Schedule::EveryBeats(3);
    let beat = Duration::from_secs(2);
    let one = schedule
        .previous_fire(at("2026-01-01T00:00:19Z"), beat)
        .unwrap();
    let two = schedule
        .previous_fire(at("2026-01-01T00:00:23Z"), beat)
        .unwrap();
    assert_eq!(one, NextFire::At(at("2026-01-01T00:00:18Z")));
    assert_eq!(one, two, "two pods inside one period compute one slot");
}

#[test]
fn a_previous_slot_of_an_unusable_schedule_is_refused_like_its_next_slot() {
    assert!(matches!(
        Schedule::EveryBeats(0).previous_fire(at("2026-01-01T00:00:00Z"), BEAT),
        Err(CronError::Schedule { .. })
    ));
    assert!(matches!(
        Schedule::Every {
            period: Duration::ZERO,
            anchor: at("2026-01-01T00:00:00Z"),
        }
        .previous_fire(at("2026-01-02T00:00:00Z"), BEAT),
        Err(CronError::Schedule { .. })
    ));
}

#[test]
fn an_expression_that_matches_no_real_date_has_no_previous_slot_either() {
    let schedule = Schedule::Cron(CronExpr::new("0 0 30 2 *").unwrap());
    assert_eq!(
        schedule
            .previous_fire(at("2026-01-01T00:00:00Z"), BEAT)
            .unwrap(),
        NextFire::Never
    );
}
