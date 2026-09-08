use std::time::Duration;

use crate::inbound::disposition::Disposition;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budgets {
    pub delivery: u32,
    pub parking: u32,
}

pub const DEFAULT_DELIVERY_BUDGET: u32 = 8;
pub const DEFAULT_PARKING_BUDGET: u32 = 10;
pub const DEFAULT_BACKOFF_BASE: Duration = Duration::from_millis(200);
pub const DEFAULT_BACKOFF_MAX: Duration = Duration::from_secs(30);

impl Default for Budgets {
    fn default() -> Self {
        Self {
            delivery: DEFAULT_DELIVERY_BUDGET,
            parking: DEFAULT_PARKING_BUDGET,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    Nak(Duration),
    DeadLetter,
}

pub fn route(
    disposition: Disposition,
    delivered: u32,
    budgets: Budgets,
    base: Duration,
    max: Duration,
) -> Route {
    match disposition {
        Disposition::Terminal => Route::DeadLetter,
        Disposition::Retry => {
            if delivered >= budgets.delivery {
                Route::DeadLetter
            } else {
                Route::Nak(backoff(delivered, base, max))
            }
        }
        Disposition::Park => {
            if delivered >= budgets.parking {
                Route::DeadLetter
            } else {
                Route::Nak(backoff(delivered, base, max))
            }
        }
    }
}

fn backoff(delivered: u32, base: Duration, max: Duration) -> Duration {
    let shift = delivered.saturating_sub(1).min(16);
    base.saturating_mul(1u32 << shift).min(max)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budgets() -> Budgets {
        Budgets {
            delivery: 4,
            parking: 3,
        }
    }

    #[test]
    fn a_terminal_disposition_dead_letters_on_the_first_delivery() {
        assert_eq!(
            route(
                Disposition::Terminal,
                1,
                budgets(),
                DEFAULT_BACKOFF_BASE,
                DEFAULT_BACKOFF_MAX
            ),
            Route::DeadLetter
        );
    }

    #[test]
    fn a_retryable_message_naks_until_its_delivery_budget_then_dead_letters() {
        for delivered in 1..4 {
            assert!(matches!(
                route(
                    Disposition::Retry,
                    delivered,
                    budgets(),
                    DEFAULT_BACKOFF_BASE,
                    DEFAULT_BACKOFF_MAX
                ),
                Route::Nak(_)
            ));
        }
        assert_eq!(
            route(
                Disposition::Retry,
                4,
                budgets(),
                DEFAULT_BACKOFF_BASE,
                DEFAULT_BACKOFF_MAX
            ),
            Route::DeadLetter
        );
    }

    #[test]
    fn a_parked_message_naks_until_its_parking_budget_then_dead_letters() {
        for delivered in 1..3 {
            assert!(matches!(
                route(
                    Disposition::Park,
                    delivered,
                    budgets(),
                    DEFAULT_BACKOFF_BASE,
                    DEFAULT_BACKOFF_MAX
                ),
                Route::Nak(_)
            ));
        }
        assert_eq!(
            route(
                Disposition::Park,
                3,
                budgets(),
                DEFAULT_BACKOFF_BASE,
                DEFAULT_BACKOFF_MAX
            ),
            Route::DeadLetter
        );
    }

    #[test]
    fn the_backoff_grows_and_is_capped() {
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(1);
        assert_eq!(backoff(1, base, max), Duration::from_millis(100));
        assert_eq!(backoff(2, base, max), Duration::from_millis(200));
        assert_eq!(backoff(3, base, max), Duration::from_millis(400));
        assert_eq!(backoff(20, base, max), max);
    }
}
