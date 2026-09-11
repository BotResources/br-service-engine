use crate::cron::{CronExpr, NextFire};
use crate::time::Timestamp;

type Vector = (&'static str, &'static str, &'static str);

const STEPS_LISTS_AND_RANGES: &[Vector] = &[
    (
        "*/15 * * * *",
        "2026-01-01T00:07:00Z",
        "2026-01-01T00:15:00Z",
    ),
    (
        "*/15 * * * *",
        "2026-01-01T00:45:00Z",
        "2026-01-01T01:00:00Z",
    ),
    (
        "0 */6 * * *",
        "2026-01-01T07:00:00Z",
        "2026-01-01T12:00:00Z",
    ),
    (
        "0 8-18/4 * * *",
        "2026-01-01T09:00:00Z",
        "2026-01-01T12:00:00Z",
    ),
    (
        "0 8-18/4 * * *",
        "2026-01-01T16:00:00Z",
        "2026-01-02T08:00:00Z",
    ),
    (
        "30 4 1,15 * *",
        "2026-01-02T00:00:00Z",
        "2026-01-15T04:30:00Z",
    ),
    ("0 0 * * *", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"),
];

const MONTH_END: &[Vector] = &[
    ("0 3 L * *", "2026-01-15T00:00:00Z", "2026-01-31T03:00:00Z"),
    ("0 3 L * *", "2026-02-01T00:00:00Z", "2026-02-28T03:00:00Z"),
    ("0 3 L * *", "2028-02-01T00:00:00Z", "2028-02-29T03:00:00Z"),
    ("0 3 L * *", "2026-04-05T00:00:00Z", "2026-04-30T03:00:00Z"),
    ("0 0 31 * *", "2026-01-31T00:00:01Z", "2026-03-31T00:00:00Z"),
];

const DAY_OF_MONTH_WITH_DAY_OF_WEEK: &[Vector] = &[
    (
        "0 0 13 * FRI",
        "2026-01-01T00:00:00Z",
        "2026-01-02T00:00:00Z",
    ),
    (
        "0 0 13 * FRI",
        "2026-01-03T00:00:00Z",
        "2026-01-09T00:00:00Z",
    ),
    (
        "0 0 13 * FRI",
        "2026-01-10T00:00:00Z",
        "2026-01-13T00:00:00Z",
    ),
    (
        "0 0 13 * +FRI",
        "2026-01-01T00:00:00Z",
        "2026-02-13T00:00:00Z",
    ),
    (
        "0 0 * * SUN",
        "2026-01-01T00:00:00Z",
        "2026-01-04T00:00:00Z",
    ),
    ("0 0 * * 7", "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z"),
    (
        "0 0 * * FRI#2",
        "2026-01-01T00:00:00Z",
        "2026-01-09T00:00:00Z",
    ),
    ("0 0 * * 5L", "2026-01-01T00:00:00Z", "2026-01-30T00:00:00Z"),
];

const NAMES: &[Vector] = &[
    (
        "0 0 1 JAN *",
        "2026-06-01T00:00:00Z",
        "2027-01-01T00:00:00Z",
    ),
    (
        "0 12 * JUL FRI",
        "2026-01-01T00:00:00Z",
        "2026-07-03T12:00:00Z",
    ),
    (
        "0 0 1 MAR-MAY *",
        "2026-03-02T00:00:00Z",
        "2026-04-01T00:00:00Z",
    ),
    (
        "0 0 1 jan *",
        "2026-06-01T00:00:00Z",
        "2027-01-01T00:00:00Z",
    ),
];

const LEAP_DAY: &[Vector] = &[
    ("0 0 29 2 *", "2026-01-01T00:00:00Z", "2028-02-29T00:00:00Z"),
    ("0 0 29 2 *", "2028-02-29T00:00:01Z", "2032-02-29T00:00:00Z"),
    ("0 0 29 2 *", "2096-03-01T00:00:00Z", "2104-02-29T00:00:00Z"),
];

fn assert_vectors(family: &str, vectors: &[Vector]) {
    for (expr, after, expected) in vectors {
        let cron = CronExpr::new(*expr).unwrap();
        let after: Timestamp = after.parse().unwrap();
        let expected: Timestamp = expected.parse().unwrap();
        assert_eq!(
            cron.next_fire(after).unwrap(),
            NextFire::At(expected),
            "{family}: {expr} after {after}"
        );
    }
}

#[test]
fn steps_lists_and_ranges_fire_where_the_pinned_vectors_say() {
    assert_vectors("steps", STEPS_LISTS_AND_RANGES);
}

#[test]
fn month_end_fires_where_the_pinned_vectors_say() {
    assert_vectors("month end", MONTH_END);
}

#[test]
fn day_of_month_combined_with_day_of_week_fires_where_the_pinned_vectors_say() {
    assert_vectors("dom with dow", DAY_OF_MONTH_WITH_DAY_OF_WEEK);
}

#[test]
fn month_and_weekday_names_fire_where_the_pinned_vectors_say() {
    assert_vectors("names", NAMES);
}

#[test]
fn leap_day_fires_where_the_pinned_vectors_say() {
    assert_vectors("leap day", LEAP_DAY);
}

fn assert_previous(family: &str, vectors: &[Vector]) {
    for (expr, _after, expected) in vectors {
        let cron = CronExpr::new(*expr).unwrap();
        let expected: Timestamp = expected.parse().unwrap();
        assert_eq!(
            cron.previous_fire(expected).unwrap(),
            NextFire::At(expected),
            "{family}: {expr} is due at {expected}"
        );
        assert_ne!(
            cron.previous_fire(expected - chrono::TimeDelta::seconds(1))
                .unwrap(),
            NextFire::At(expected),
            "{family}: {expr} is not yet due one second before {expected}"
        );
    }
}

#[test]
fn the_backward_search_lands_on_the_same_lattice_as_the_forward_one() {
    assert_previous("steps", STEPS_LISTS_AND_RANGES);
    assert_previous("month end", MONTH_END);
    assert_previous("dom with dow", DAY_OF_MONTH_WITH_DAY_OF_WEEK);
    assert_previous("names", NAMES);
    assert_previous("leap day", LEAP_DAY);
}
