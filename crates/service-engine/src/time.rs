use std::fmt;
use std::ops::{Add, Sub};
use std::str::FromStr;

use chrono::{DateTime, ParseError, SubsecRound, TimeDelta, Utc};

const POSTGRES_SUBSEC_DIGITS: u16 = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct Timestamp(DateTime<Utc>);

impl Timestamp {
    pub const UNIX_EPOCH: Self = Self(DateTime::<Utc>::UNIX_EPOCH);
    pub const MIN: Self = Self(DateTime::<Utc>::MIN_UTC);

    pub fn from_utc(value: DateTime<Utc>) -> Self {
        Self(value.trunc_subsecs(POSTGRES_SUBSEC_DIGITS))
    }

    pub const fn as_datetime(self) -> DateTime<Utc> {
        self.0
    }

    pub fn checked_add_signed(self, delta: TimeDelta) -> Option<Self> {
        self.0.checked_add_signed(delta).map(Self::from_utc)
    }

    pub fn checked_sub_signed(self, delta: TimeDelta) -> Option<Self> {
        self.0.checked_sub_signed(delta).map(Self::from_utc)
    }

    pub fn signed_duration_since(self, earlier: Self) -> TimeDelta {
        self.0.signed_duration_since(earlier.0)
    }
}

impl Sub for Timestamp {
    type Output = TimeDelta;

    fn sub(self, rhs: Self) -> TimeDelta {
        self.0 - rhs.0
    }
}

impl Sub<TimeDelta> for Timestamp {
    type Output = Self;

    fn sub(self, rhs: TimeDelta) -> Self {
        Self::from_utc(self.0 - rhs)
    }
}

impl Add<TimeDelta> for Timestamp {
    type Output = Self;

    fn add(self, rhs: TimeDelta) -> Self {
        Self::from_utc(self.0 + rhs)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for Timestamp {
    type Err = ParseError;

    fn from_str(value: &str) -> Result<Self, ParseError> {
        DateTime::<Utc>::from_str(value).map(Self::from_utc)
    }
}

pub fn now() -> Timestamp {
    Timestamp::from_utc(Utc::now())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nanos_of(ts: Timestamp) -> u32 {
        ts.as_datetime().timestamp_subsec_nanos()
    }

    #[test]
    fn a_nanosecond_bearing_instant_is_truncated_to_postgres_microsecond_precision() {
        let raw = DateTime::<Utc>::UNIX_EPOCH
            + TimeDelta::microseconds(322_703)
            + TimeDelta::nanoseconds(503);
        let ts = Timestamp::from_utc(raw);
        assert_eq!(nanos_of(ts) % 1_000, 0);
        assert_eq!(nanos_of(ts), 322_703_000);
    }

    #[test]
    fn truncation_round_trips_equal_to_the_postgres_stored_value() {
        let raw = Timestamp::UNIX_EPOCH
            + TimeDelta::seconds(1_800_000_000)
            + TimeDelta::nanoseconds(322_703_503);
        let stored = Timestamp::from_utc(
            DateTime::<Utc>::UNIX_EPOCH
                + TimeDelta::seconds(1_800_000_000)
                + TimeDelta::microseconds(322_703),
        );
        assert_eq!(raw, stored);
    }

    #[test]
    fn arithmetic_never_reintroduces_sub_microsecond_precision() {
        let base = Timestamp::from_utc(DateTime::<Utc>::UNIX_EPOCH + TimeDelta::microseconds(500));
        let shifted = base + TimeDelta::nanoseconds(1_999);
        assert_eq!(nanos_of(shifted) % 1_000, 0);
        let back = shifted - TimeDelta::nanoseconds(1_999);
        assert_eq!(nanos_of(back) % 1_000, 0);
    }
}
